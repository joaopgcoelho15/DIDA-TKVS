
package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsMain;
import dadkvs.DadkvsServer;
import dadkvs.DadkvsServerServiceGrpc;
import dadkvs.DadkvsServerServiceGrpc.DadkvsServerServiceStub;

import io.grpc.stub.StreamObserver;

import java.util.List;

public class DadkvsPaxosServiceImpl extends DadkvsServerServiceGrpc.DadkvsServerServiceImplBase {


    DadkvsServerState server_state;
    int leaderStamp;
    int proposedValue;
    List<DadkvsServerServiceStub> stubs;
    DadkvsMainServiceImpl mainService;


    public DadkvsPaxosServiceImpl(DadkvsServerState state, List<DadkvsServerServiceStub> stubs, DadkvsMainServiceImpl mainService) {
        this.server_state = state;
        leaderStamp = -1;
        proposedValue = -1;
        this.stubs = stubs;
        this.mainService = mainService;
    }


    @Override
    public void phaseone(DadkvsServer.PhaseOneRequest request, StreamObserver<DadkvsServer.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase1 request: " + request);

        int currentStamp = request.getPhase1Timestamp();

        DadkvsServer.PhaseOneReply response;

        //If this proposer has an ID higher then any ID I have promised
        if(currentStamp > leaderStamp){
            //If a value has already been accepted previously
            if(proposedValue >= 0){
                //Send PROMISE IDp accepted IDa, value
                response = DadkvsServer.PhaseOneReply.newBuilder()
                .setPhase1Accepted(true).setPhase1Timestamp(leaderStamp).setPhase1Value(proposedValue).build();
            }
            else{
                //Send PROMISE IDp
                response = DadkvsServer.PhaseOneReply.newBuilder()
                .setPhase1Accepted(true).setPhase1Timestamp(-1).setPhase1Value(-1).build();
            }
            leaderStamp = currentStamp;
        }
        else{
            //Ignore the request
            response = DadkvsServer.PhaseOneReply.newBuilder()
                .setPhase1Accepted(false).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsServer.PhaseTwoRequest request, StreamObserver<DadkvsServer.PhaseTwoReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase two request: " + request);

        int currentStamp = request.getPhase2Timestamp();

        DadkvsServer.PhaseTwoReply response;

        if(currentStamp > leaderStamp){
            leaderStamp = currentStamp;
            //Reply ACCEPT IDp, value
            response = DadkvsServer.PhaseTwoReply.newBuilder()
                .setPhase2Accepted(true).build();
            //Also broadcast to all learners

        }
        else{
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
        System.out.println("Receive learn request: " + request);

        int reqid = request.getLearnvalue();
        int timestamp = request.getLearntimestamp();
        boolean result = false;

        if(timestamp > leaderStamp){
            leaderStamp = timestamp;
            proposedValue = reqid;
        
            //If the queue is empty, it means that if I have the request I should do it now
            if (server_state.idQueue.isEmpty()){
                server_state.idQueue.add(reqid);
                DadkvsMain.CommitRequest pendingRequest = searchRequest(reqid);
                if(pendingRequest != null){
                    mainService.committx(pendingRequest, server_state.pendingRequests.remove(pendingRequest));
                }
            }
            else {
                server_state.idQueue.add(reqid);
            }
            result = true;
        }
        else{
            //Ignore the request
            result = false;
        }
    
        DadkvsServer.LearnReply response = DadkvsServer.LearnReply.newBuilder()
                .setLearnaccepted(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public DadkvsMain.CommitRequest searchRequest(int reqid){
        for (DadkvsMain.CommitRequest pendingRequest : server_state.pendingRequests.keySet()) {
            //If the incoming request is stored and 
            if (pendingRequest.getReqid() == reqid && reqid == server_state.idQueue.peekFirst()) {
                return pendingRequest;
            }
        } 
        return null;
    }

}
