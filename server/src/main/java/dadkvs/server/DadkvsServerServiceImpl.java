package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsServer;
import dadkvs.DadkvsServerServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DadkvsServerServiceImpl extends DadkvsServerServiceGrpc.DadkvsServerServiceImplBase {

    DadkvsServerState server_state;
    DadkvsMainServiceImpl mainService;

    public DadkvsServerServiceImpl(DadkvsServerState state, DadkvsMainServiceImpl mainService) {
        this.server_state = state;
        this.mainService = mainService;
    }

    /**
     * This method is called when a server receives a broadcast message from the leader.
     *
     * @param request
     */
    @Override
    public void reqidbroadcast(DadkvsServer.ReqIdBroadcast request, StreamObserver<DadkvsServer.BroadcastReply> responseObserver) {
        System.out.println("Receiving reqid broadcast:" + request);

        int reqid = request.getReqid();

        //iterate over server_state.pendingRequests and check if there is a request with reqid

        //If this is the first request from the leader, just change the variable
        if (server_state.nextReqid == -1){
            server_state.nextReqid = reqid;
            DadkvsMain.CommitRequest pendingRequest = searchRequest(reqid);
            if(pendingRequest != null){
                mainService.committx(pendingRequest, server_state.pendingRequests.remove(pendingRequest));
            }
        }
        else {
            server_state.idQueue.add(reqid);
        }
    
        boolean result = true;
        DadkvsServer.BroadcastReply response = DadkvsServer.BroadcastReply.newBuilder()
                .setAck(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public DadkvsMain.CommitRequest searchRequest(int reqid){
        for (DadkvsMain.CommitRequest pendingRequest : server_state.pendingRequests.keySet()) {
            //If the incoming request is stored and 
            if (pendingRequest.getReqid() == reqid && reqid == server_state.nextReqid) {
                return pendingRequest;
            }
        } 
        return null;
    }
}
