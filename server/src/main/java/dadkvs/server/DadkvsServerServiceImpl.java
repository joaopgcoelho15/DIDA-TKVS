package dadkvs.server;

import com.google.protobuf.Empty;
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
    public void reqidbroadcast(DadkvsServer.ReqIdBroadcast request, StreamObserver<Empty> responseObserver) {
        System.out.println("Receiving reqid broadcast:" + request);

        int reqid = request.getReqid();

        //iterate over server_state.pendingRequests and check if there is a request with reqid

        //If this is the first request from the leader, just change the variable
        if (server_state.nextReqid == -1){
            server_state.nextReqid = reqid;
        }
        else{
            //Check if the request is already stored
            for (DadkvsMain.CommitRequest pendingRequest : server_state.pendingRequests.keySet()) {
                if (pendingRequest.getReqid() == reqid) {
                    mainService.committx(pendingRequest, server_state.pendingRequests.remove(pendingRequest));
                    return;
                }
            }    
            // if the request is not found, add it to the queue
            this.server_state.idQueue.add(reqid);
        }
    }
}
