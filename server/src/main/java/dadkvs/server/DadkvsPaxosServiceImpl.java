
package dadkvs.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsServer;
import dadkvs.DadkvsServerServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsServerServiceGrpc.DadkvsServerServiceImplBase {


    DadkvsServerState server_state;
    List<Integer> leaderStamp_read;
    List<Integer> leaderStamp_write;
    List<Integer> proposedValue;
    List<Boolean> alreadyCommited;
    int nServers;

    HashMap<Integer, dadkvs.DadkvsServerServiceGrpc.DadkvsServerServiceStub> stubs;
    DadkvsMainServiceImpl mainService;

    public DadkvsPaxosServiceImpl(DadkvsServerState state, HashMap<Integer, dadkvs.DadkvsServerServiceGrpc.DadkvsServerServiceStub> stubs, DadkvsMainServiceImpl mainService) {
        this.server_state = state;
        leaderStamp_read = new ArrayList<>(1000);
        leaderStamp_write = new ArrayList<>(1000);
        leaderStamp_read.addAll(Collections.nCopies(1000, -1));
        leaderStamp_write.addAll(Collections.nCopies(1000, -1));
        proposedValue = new ArrayList<>(1000);
        proposedValue.addAll(Collections.nCopies(1000, -1));
        alreadyCommited = new ArrayList<>(1000);
        alreadyCommited.addAll(Collections.nCopies(1000, false));
        nServers = 5;
        this.stubs = stubs;
        this.mainService = mainService;
    }


    @Override
    public void phaseone(DadkvsServer.PhaseOneRequest request, StreamObserver<DadkvsServer.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("Receiving phase1 request: " + request);

        int currentStamp = request.getPhase1Timestamp();
        int paxosRun = request.getPhase1Index();
        server_state.currentPaxosRun = paxosRun;

        DadkvsServer.PhaseOneReply response;

        //If this proposer has an ID higher then any ID I have promised
        if (currentStamp > leaderStamp_read.get(paxosRun) && currentStamp > leaderStamp_write.get(paxosRun)) {
            //If a value has already been accepted previously
            if (proposedValue.get(paxosRun) >= 0) {
                //Send PROMISE IDp accepted IDa, value
                response = DadkvsServer.PhaseOneReply.newBuilder()
                        .setPhase1Accepted(true).setPhase1Timestamp(leaderStamp_read.get(paxosRun)).setPhase1Value(proposedValue.get(paxosRun)).setPhase1Index(paxosRun).build();
            } else {
                //Send PROMISE IDp
                response = DadkvsServer.PhaseOneReply.newBuilder()
                        .setPhase1Accepted(true).setPhase1Timestamp(-1).setPhase1Value(-1).setPhase1Index(paxosRun).build();
            }
            leaderStamp_read.set(paxosRun, currentStamp);
        } else {
            //Ignore the request
            response = DadkvsServer.PhaseOneReply.newBuilder()
                    .setPhase1Accepted(false).build();
        }

        responseObserver.onNext(response);
        System.out.println("Sending phase1 response: " + response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsServer.PhaseTwoRequest request, StreamObserver<DadkvsServer.PhaseTwoReply> responseObserver) {
        // for debug purposes
        System.out.println("Receiving phase two request: " + request);

        int currentStamp = request.getPhase2Timestamp();
        int value = request.getPhase2Value();
        int paxosRun = request.getPhase2Index();

        DadkvsServer.PhaseTwoReply response;

        if (currentStamp > leaderStamp_write.get(paxosRun) && currentStamp > leaderStamp_read.get(paxosRun)) {
            leaderStamp_write.set(paxosRun, currentStamp);
            //Store the agreed value
            proposedValue.set(paxosRun, value);

            //Reply ACCEPT IDp, value
            response = DadkvsServer.PhaseTwoReply.newBuilder()
                    .setPhase2Accepted(true).setPhase2Index(paxosRun).build();
            //Also broadcast to all onlyLearners
            broadcastToLearners(value, currentStamp, paxosRun);
        } else {
            //Ignore the request
            response = DadkvsServer.PhaseTwoReply.newBuilder()
                    .setPhase2Accepted(false).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void learn(DadkvsServer.LearnRequest request, StreamObserver<DadkvsServer.LearnReply> responseObserver) {
        // for debug purposes
        System.out.println("Receiving learn request: " + request);

        int reqid = request.getLearnvalue();
        int timestamp = request.getLearntimestamp();
        int paxosRun = request.getLearnindex();
        boolean result;

        if (timestamp >= leaderStamp_write.get(paxosRun)) {
            leaderStamp_write.set(paxosRun, timestamp);
            proposedValue.set(paxosRun, reqid);

            //If the queue is empty, it means that if I have the request I should do it now
            if (server_state.idQueue.isEmpty()) {
                server_state.idQueue.add(reqid);
                server_state.just_commit = true;
                DadkvsMain.CommitRequest pendingRequest = searchRequest(reqid);
                if (pendingRequest != null) {
                    mainService.committx(pendingRequest, server_state.pendingRequests.remove(pendingRequest));
                }
                server_state.just_commit = false;
            } else {
                //Just to be sure we dont add it to the queue multiple times
                if (!server_state.idQueue.contains(reqid)) {
                    server_state.idQueue.add(reqid);
                }
            }

            alreadyCommited.set(paxosRun, true);
            result = true;
        } else {
            //Ignore the request
            result = false;
        }

        DadkvsServer.LearnReply response = DadkvsServer.LearnReply.newBuilder()
                .setLearnaccepted(result).setLearnindex(paxosRun).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public DadkvsMain.CommitRequest searchRequest(int reqId) {
        for (DadkvsMain.CommitRequest pendingRequest : server_state.pendingRequests.keySet()) {
            //If the incoming request is stored and 
            if (pendingRequest.getReqid() == reqId && reqId == server_state.idQueue.peekFirst()) {
                return pendingRequest;
            }
        }
        return null;
    }

    public void broadcastToLearners(int value, int timestamp, int paxosRun) {
        DadkvsServer.LearnRequest learnRequest = DadkvsServer.LearnRequest.newBuilder().setLearnvalue(value).setLearntimestamp(timestamp).setLearnindex(paxosRun).build();

        ArrayList<DadkvsServer.LearnReply> learnRequests = new ArrayList<>();
        GenericResponseCollector<DadkvsServer.LearnReply> learn_collector = new GenericResponseCollector<>(learnRequests, 4);

        Context ctx = Context.current().fork();

        ctx.run(() -> {
            for (int i = 0; i < nServers; i++) {
                //Send the consensus value to all the learners
                if (i == server_state.my_id) {
                    continue;
                }
                CollectorStreamObserver<DadkvsServer.LearnReply> learn_observer = new CollectorStreamObserver<>(learn_collector);
                stubs.get(i).learn(learnRequest, learn_observer);
            }
        });
    }
}
