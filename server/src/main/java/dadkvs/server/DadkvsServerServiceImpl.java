package dadkvs.server;

import com.google.protobuf.Empty;
import dadkvs.DadkvsMain;
import dadkvs.DadkvsServer;
import dadkvs.DadkvsServerServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DadkvsServerServiceImpl extends DadkvsServerServiceGrpc.DadkvsServerServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsServerServiceImpl(DadkvsServerState state) {
        this.server_state = state;
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

        for (DadkvsMain.CommitRequest pendingRequest : server_state.pendingRequests.keySet()) {
            if (pendingRequest.getReqid() == reqid) {
                // process the request
                StreamObserver<DadkvsMain.CommitReply> clientObserver = server_state.pendingRequests.remove(pendingRequest);

                // for debug purposes
                System.out.println("Processing pending request:" + pendingRequest);

                int key1 = pendingRequest.getKey1();
                int version1 = pendingRequest.getVersion1();
                int key2 = pendingRequest.getKey2();
                int version2 = pendingRequest.getVersion2();
                int writekey = pendingRequest.getWritekey();
                int writeval = pendingRequest.getWriteval();

                // for debug purposes
                System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

                // send the response
                DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder().setReqid(reqid).build();
                clientObserver.onNext(response);
                clientObserver.onCompleted();

                return;
            }
        }

        // if the request is not found, add it to the queue
        this.server_state.idQueue.add(reqid);
    }
}
