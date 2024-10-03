package dadkvs.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class DadkvsServerState {
    boolean i_am_leader;
    boolean paxosRunning;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    int paxosStamp;
    ArrayList<Integer> finalPaxosValue;
    int currentConfig;

    LinkedList<Integer> idQueue;
    List<Integer> onlyLearners;

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
        paxosStamp = my_id;
        onlyLearners = new ArrayList<>();
        finalPaxosValue = new ArrayList<>(1000);
        finalPaxosValue.addAll(java.util.Collections.nCopies(1000, -1));
    }

    public void addPendingRequest(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        pendingRequests.put(request, responseObserver);
    }
}
