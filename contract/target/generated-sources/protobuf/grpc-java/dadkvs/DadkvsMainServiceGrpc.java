package dadkvs;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.36.0)",
    comments = "Source: DadkvsMain.proto")
public final class DadkvsMainServiceGrpc {

  private DadkvsMainServiceGrpc() {}

  public static final String SERVICE_NAME = "dadkvs.DadkvsMainService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsMain.ReadRequest,
      dadkvs.DadkvsMain.ReadReply> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "read",
      requestType = dadkvs.DadkvsMain.ReadRequest.class,
      responseType = dadkvs.DadkvsMain.ReadReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsMain.ReadRequest,
      dadkvs.DadkvsMain.ReadReply> getReadMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsMain.ReadRequest, dadkvs.DadkvsMain.ReadReply> getReadMethod;
    if ((getReadMethod = DadkvsMainServiceGrpc.getReadMethod) == null) {
      synchronized (DadkvsMainServiceGrpc.class) {
        if ((getReadMethod = DadkvsMainServiceGrpc.getReadMethod) == null) {
          DadkvsMainServiceGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsMain.ReadRequest, dadkvs.DadkvsMain.ReadReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsMain.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsMain.ReadReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsMainServiceMethodDescriptorSupplier("read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsMain.CommitRequest,
      dadkvs.DadkvsMain.CommitReply> getCommittxMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "committx",
      requestType = dadkvs.DadkvsMain.CommitRequest.class,
      responseType = dadkvs.DadkvsMain.CommitReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsMain.CommitRequest,
      dadkvs.DadkvsMain.CommitReply> getCommittxMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsMain.CommitRequest, dadkvs.DadkvsMain.CommitReply> getCommittxMethod;
    if ((getCommittxMethod = DadkvsMainServiceGrpc.getCommittxMethod) == null) {
      synchronized (DadkvsMainServiceGrpc.class) {
        if ((getCommittxMethod = DadkvsMainServiceGrpc.getCommittxMethod) == null) {
          DadkvsMainServiceGrpc.getCommittxMethod = getCommittxMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsMain.CommitRequest, dadkvs.DadkvsMain.CommitReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "committx"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsMain.CommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsMain.CommitReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsMainServiceMethodDescriptorSupplier("committx"))
              .build();
        }
      }
    }
    return getCommittxMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DadkvsMainServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceStub>() {
        @java.lang.Override
        public DadkvsMainServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsMainServiceStub(channel, callOptions);
        }
      };
    return DadkvsMainServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DadkvsMainServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceBlockingStub>() {
        @java.lang.Override
        public DadkvsMainServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsMainServiceBlockingStub(channel, callOptions);
        }
      };
    return DadkvsMainServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DadkvsMainServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsMainServiceFutureStub>() {
        @java.lang.Override
        public DadkvsMainServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsMainServiceFutureStub(channel, callOptions);
        }
      };
    return DadkvsMainServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class DadkvsMainServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void read(dadkvs.DadkvsMain.ReadRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.ReadReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    public void committx(dadkvs.DadkvsMain.CommitRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.CommitReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommittxMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getReadMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsMain.ReadRequest,
                dadkvs.DadkvsMain.ReadReply>(
                  this, METHODID_READ)))
          .addMethod(
            getCommittxMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsMain.CommitRequest,
                dadkvs.DadkvsMain.CommitReply>(
                  this, METHODID_COMMITTX)))
          .build();
    }
  }

  /**
   */
  public static final class DadkvsMainServiceStub extends io.grpc.stub.AbstractAsyncStub<DadkvsMainServiceStub> {
    private DadkvsMainServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsMainServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsMainServiceStub(channel, callOptions);
    }

    /**
     */
    public void read(dadkvs.DadkvsMain.ReadRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.ReadReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void committx(dadkvs.DadkvsMain.CommitRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.CommitReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommittxMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DadkvsMainServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<DadkvsMainServiceBlockingStub> {
    private DadkvsMainServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsMainServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsMainServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public dadkvs.DadkvsMain.ReadReply read(dadkvs.DadkvsMain.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public dadkvs.DadkvsMain.CommitReply committx(dadkvs.DadkvsMain.CommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommittxMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DadkvsMainServiceFutureStub extends io.grpc.stub.AbstractFutureStub<DadkvsMainServiceFutureStub> {
    private DadkvsMainServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsMainServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsMainServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsMain.ReadReply> read(
        dadkvs.DadkvsMain.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsMain.CommitReply> committx(
        dadkvs.DadkvsMain.CommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommittxMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_READ = 0;
  private static final int METHODID_COMMITTX = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DadkvsMainServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DadkvsMainServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ:
          serviceImpl.read((dadkvs.DadkvsMain.ReadRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.ReadReply>) responseObserver);
          break;
        case METHODID_COMMITTX:
          serviceImpl.committx((dadkvs.DadkvsMain.CommitRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsMain.CommitReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class DadkvsMainServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DadkvsMainServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return dadkvs.DadkvsMain.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DadkvsMainService");
    }
  }

  private static final class DadkvsMainServiceFileDescriptorSupplier
      extends DadkvsMainServiceBaseDescriptorSupplier {
    DadkvsMainServiceFileDescriptorSupplier() {}
  }

  private static final class DadkvsMainServiceMethodDescriptorSupplier
      extends DadkvsMainServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DadkvsMainServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (DadkvsMainServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DadkvsMainServiceFileDescriptorSupplier())
              .addMethod(getReadMethod())
              .addMethod(getCommittxMethod())
              .build();
        }
      }
    }
    return result;
  }
}
