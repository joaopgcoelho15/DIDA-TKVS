package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.LinkedList;

public class DadkvsServerState {
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;

    LinkedList<Integer> idQueue;

    HashMap<DadkvsMain.CommitRequest, StreamObserver<DadkvsMain.CommitReply>> pendingRequests;


    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;


    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread(main_loop);
        main_loop_worker.start();
        pendingRequests = new HashMap<>();
        idQueue = new LinkedList<>();
    }

    public void addPendingRequest(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        pendingRequests.put(request, responseObserver);
    }
}
