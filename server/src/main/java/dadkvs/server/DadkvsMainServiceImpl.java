package dadkvs.server;

/* these imported classes are generated by the contract */

import java.util.ArrayList;
import java.util.HashMap;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsServer;
import dadkvs.DadkvsServerServiceGrpc.DadkvsServerServiceStub;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

    DadkvsServerState server_state;
    int timestamp;
    HashMap<Integer, DadkvsServerServiceStub> stubs;
    int nAcceptors;
    int paxosRun;

    public DadkvsMainServiceImpl(DadkvsServerState state, HashMap<Integer, DadkvsServerServiceStub> stubs) {
        this.server_state = state;
        this.timestamp = 0;
        this.stubs = stubs;
        nAcceptors = 2;
        paxosRun = 0;
    }

    @Override
    public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
        // for debug purposes
        System.out.println("Receiving read request:" + request);

        int reqid = request.getReqid();
        int key = request.getKey();
        VersionedValue vv = this.server_state.store.read(key);

        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
                .setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        // for debug purposes
        System.out.println("Receiving commit request:" + request);

        int reqid = request.getReqid();

        if(server_state.i_am_leader){
            innitPaxos(stubs, reqid);
        }

        //If the queue is empty, store the request and return
        else if(server_state.idQueue.isEmpty()){
            server_state.addPendingRequest(request, responseObserver);
            return;
        }
        //If this request is not the next to be processed
        else if(reqid != server_state.idQueue.peekFirst()) {
            server_state.addPendingRequest(request, responseObserver);
            return;
        }

        int key1 = request.getKey1();
        int version1 = request.getVersion1();
        int key2 = request.getKey2();
        int version2 = request.getVersion2();
        int writekey = request.getWritekey();
        int writeval = request.getWriteval();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);

        // for debug purposes
        System.out.println("Result is ready for request with reqid " + reqid);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(reqid).setAck(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        if(!server_state.idQueue.isEmpty()){
            server_state.idQueue.removeFirst();
        }     
    }

    public void innitPaxos(HashMap<Integer, DadkvsServerServiceStub> stubs, int reqid){
        DadkvsServer.PhaseOneRequest proposeRequest = DadkvsServer.PhaseOneRequest.newBuilder().setPhase1Timestamp(server_state.paxosStamp).setPhase1Index(paxosRun).build();

        ArrayList<DadkvsServer.PhaseOneReply> promises = new ArrayList<>();
        GenericResponseCollector<DadkvsServer.PhaseOneReply> promises_collector = new GenericResponseCollector<>(promises, 2);

        for (int i = 0; i < nAcceptors; i++ ) {
            //Send the PROPOSE message to all the acceptors
            if(server_state.onlyLearners.contains(i) || i == server_state.my_id){
                continue;
            }
            CollectorStreamObserver<DadkvsServer.PhaseOneReply> p1_observer = new CollectorStreamObserver<>(promises_collector);
            stubs.get(i).phaseone(proposeRequest, p1_observer);
        }
        promises_collector.waitForTarget(2);

        int acceptedPrepares = 0;
        int acceptedValue = -1;
        int newValue = -1;
        int highestID = -1;

        if (promises.size() > 2) {

            for (DadkvsServer.PhaseOneReply promise : promises) {
                //If the PREPARE was accepted
                if (promise.getPhase1Accepted()) {
                    acceptedPrepares++;
                    acceptedValue = promise.getPhase1Value();
                    //If there was already a commited value this leader adopts this value
                    if (acceptedValue != -1 && promise.getPhase1Timestamp() > highestID) {
                        newValue = acceptedValue;
                        highestID = promise.getPhase1Timestamp();
                    }
                }
            }
            //If majority is accepted go to phase 2
            if (acceptedPrepares > 0) {
                if(newValue != -1){
                    proposerPhase2(reqid);  
                }
                else{
                    proposerPhase2(newValue);
                }
            }
            //If the prepare request was not accepted, try again with a new timestamp
            else{
                server_state.paxosStamp += 3;
                innitPaxos(stubs, reqid);
            }
        }
        else
            System.out.println("Panic...error preparing");
    }

    public void proposerPhase2(int value){
        DadkvsServer.PhaseTwoRequest proposeRequest = DadkvsServer.PhaseTwoRequest.newBuilder().setPhase2Timestamp(server_state.paxosStamp).setPhase2Value(value).setPhase2Index(paxosRun).build();

        ArrayList<DadkvsServer.PhaseTwoReply> acceptRequests = new ArrayList<>();
        GenericResponseCollector<DadkvsServer.PhaseTwoReply> acceptRequests_collector = new GenericResponseCollector<>(acceptRequests, 4);

        for (int i = 0; i < nAcceptors; i++ ) {
            //Send the ACCEPT-REQUEST message to all the acceptors
            if(server_state.onlyLearners.contains(i) || i == server_state.my_id){
                continue;
            }
            CollectorStreamObserver<DadkvsServer.PhaseTwoReply> p2_observer = new CollectorStreamObserver<>(acceptRequests_collector);
            stubs.get(i).phasetwo(proposeRequest, p2_observer);
        }
        acceptRequests_collector.waitForTarget(2);

        int acceptedPrepares = 0;

        if (acceptRequests.size() > 2) {

            for (DadkvsServer.PhaseTwoReply acceptReq : acceptRequests) {
                //If the REQUEST-ACCEPT was accepted
                if (acceptReq.getPhase2Accepted()) {
                    acceptedPrepares++;
                }
            }
            //If majority is accepted a new consensus is reached
            if (acceptedPrepares > 2) {
                server_state.finalPaxosValue.add(paxosRun, value);
                paxosRun++;
            }
        }
        else
            System.out.println("Panic...error commiting");
    }
}
