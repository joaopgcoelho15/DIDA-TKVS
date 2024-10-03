package dadkvs.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dadkvs.DadkvsServerServiceGrpc;
import dadkvs.DadkvsServerServiceGrpc.DadkvsServerServiceStub;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;



public class DadkvsServer {

    static DadkvsServerState server_state;

    /**
     * Server host port.
     */
    private static int port;

    public static void main(String[] args) throws Exception {
        final int kvsize = 1000;
        final int n_servers = 5;

        System.out.println(DadkvsServer.class.getSimpleName());

        // Print received arguments.
        System.out.printf("Received %d arguments%n", args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.printf("arg[%d] = %s%n", i, args[i]);
        }

        // Check arguments.
        if (args.length < 2) {
            System.err.println("Argument(s) missing!");
            System.err.printf("Usage: java %s baseport replica-id%n", Server.class.getName());
            return;
        }

        int base_port = Integer.valueOf(args[0]);
        int my_id = Integer.valueOf(args[1]);

        server_state = new DadkvsServerState(kvsize, base_port, my_id);

        port = base_port + my_id;

        HashMap<Integer, DadkvsServerServiceStub> stubs = new HashMap<>();
        for (int i = 0; i < n_servers; i++) {
            if (i != my_id) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", base_port + i).usePlaintext().build();
                stubs.put(i, DadkvsServerServiceGrpc.newStub(channel));
            }
        }

        final BindableService console_impl = new DadkvsConsoleServiceImpl(server_state);
        final BindableService service_impl = new DadkvsMainServiceImpl(server_state, stubs);
        final BindableService paxos_impl = new DadkvsPaxosServiceImpl(server_state, stubs, (DadkvsMainServiceImpl)service_impl);
        final BindableService server_service_impl = new DadkvsServerServiceImpl(server_state, (DadkvsMainServiceImpl) service_impl);

        server_state.currentConfig = server_state.store.read(0).getValue();

        switch (server_state.currentConfig) {
            case 0 -> server_state.onlyLearners.addAll(List.of(2, 3));
            case 1 -> server_state.onlyLearners.addAll(List.of(3, 0));
            case 2 -> server_state.onlyLearners.addAll(List.of(0, 1));
            default -> {
            }
        }

        // Create a new server to listen on port.
        Server server = ServerBuilder.forPort(port).addService(service_impl).addService(console_impl).addService(paxos_impl).addService(server_service_impl).build();
        // Start the server.
        server.start();
        // Server threads are running in the background.
        System.out.println("Server started");

        // Do not exit the main thread. Wait until server is terminated.
        server.awaitTermination();
    }
}
