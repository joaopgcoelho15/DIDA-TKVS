package dadkvs;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.36.0)",
    comments = "Source: DadkvsPaxos.proto")
public final class DadkvsPaxosServiceGrpc {

  private DadkvsPaxosServiceGrpc() {}

  public static final String SERVICE_NAME = "dadkvs.DadkvsPaxosService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseOneRequest,
      dadkvs.DadkvsPaxos.PhaseOneReply> getPhaseoneMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "phaseone",
      requestType = dadkvs.DadkvsPaxos.PhaseOneRequest.class,
      responseType = dadkvs.DadkvsPaxos.PhaseOneReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseOneRequest,
      dadkvs.DadkvsPaxos.PhaseOneReply> getPhaseoneMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseOneRequest, dadkvs.DadkvsPaxos.PhaseOneReply> getPhaseoneMethod;
    if ((getPhaseoneMethod = DadkvsPaxosServiceGrpc.getPhaseoneMethod) == null) {
      synchronized (DadkvsPaxosServiceGrpc.class) {
        if ((getPhaseoneMethod = DadkvsPaxosServiceGrpc.getPhaseoneMethod) == null) {
          DadkvsPaxosServiceGrpc.getPhaseoneMethod = getPhaseoneMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsPaxos.PhaseOneRequest, dadkvs.DadkvsPaxos.PhaseOneReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "phaseone"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.PhaseOneRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.PhaseOneReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsPaxosServiceMethodDescriptorSupplier("phaseone"))
              .build();
        }
      }
    }
    return getPhaseoneMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseTwoRequest,
      dadkvs.DadkvsPaxos.PhaseTwoReply> getPhasetwoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "phasetwo",
      requestType = dadkvs.DadkvsPaxos.PhaseTwoRequest.class,
      responseType = dadkvs.DadkvsPaxos.PhaseTwoReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseTwoRequest,
      dadkvs.DadkvsPaxos.PhaseTwoReply> getPhasetwoMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.PhaseTwoRequest, dadkvs.DadkvsPaxos.PhaseTwoReply> getPhasetwoMethod;
    if ((getPhasetwoMethod = DadkvsPaxosServiceGrpc.getPhasetwoMethod) == null) {
      synchronized (DadkvsPaxosServiceGrpc.class) {
        if ((getPhasetwoMethod = DadkvsPaxosServiceGrpc.getPhasetwoMethod) == null) {
          DadkvsPaxosServiceGrpc.getPhasetwoMethod = getPhasetwoMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsPaxos.PhaseTwoRequest, dadkvs.DadkvsPaxos.PhaseTwoReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "phasetwo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.PhaseTwoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.PhaseTwoReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsPaxosServiceMethodDescriptorSupplier("phasetwo"))
              .build();
        }
      }
    }
    return getPhasetwoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.LearnRequest,
      dadkvs.DadkvsPaxos.LearnReply> getLearnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "learn",
      requestType = dadkvs.DadkvsPaxos.LearnRequest.class,
      responseType = dadkvs.DadkvsPaxos.LearnReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.LearnRequest,
      dadkvs.DadkvsPaxos.LearnReply> getLearnMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsPaxos.LearnRequest, dadkvs.DadkvsPaxos.LearnReply> getLearnMethod;
    if ((getLearnMethod = DadkvsPaxosServiceGrpc.getLearnMethod) == null) {
      synchronized (DadkvsPaxosServiceGrpc.class) {
        if ((getLearnMethod = DadkvsPaxosServiceGrpc.getLearnMethod) == null) {
          DadkvsPaxosServiceGrpc.getLearnMethod = getLearnMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsPaxos.LearnRequest, dadkvs.DadkvsPaxos.LearnReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "learn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.LearnRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsPaxos.LearnReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsPaxosServiceMethodDescriptorSupplier("learn"))
              .build();
        }
      }
    }
    return getLearnMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DadkvsPaxosServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceStub>() {
        @java.lang.Override
        public DadkvsPaxosServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsPaxosServiceStub(channel, callOptions);
        }
      };
    return DadkvsPaxosServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DadkvsPaxosServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceBlockingStub>() {
        @java.lang.Override
        public DadkvsPaxosServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsPaxosServiceBlockingStub(channel, callOptions);
        }
      };
    return DadkvsPaxosServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DadkvsPaxosServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsPaxosServiceFutureStub>() {
        @java.lang.Override
        public DadkvsPaxosServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsPaxosServiceFutureStub(channel, callOptions);
        }
      };
    return DadkvsPaxosServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class DadkvsPaxosServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void phaseone(dadkvs.DadkvsPaxos.PhaseOneRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseOneReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPhaseoneMethod(), responseObserver);
    }

    /**
     */
    public void phasetwo(dadkvs.DadkvsPaxos.PhaseTwoRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseTwoReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPhasetwoMethod(), responseObserver);
    }

    /**
     */
    public void learn(dadkvs.DadkvsPaxos.LearnRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.LearnReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLearnMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPhaseoneMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsPaxos.PhaseOneRequest,
                dadkvs.DadkvsPaxos.PhaseOneReply>(
                  this, METHODID_PHASEONE)))
          .addMethod(
            getPhasetwoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsPaxos.PhaseTwoRequest,
                dadkvs.DadkvsPaxos.PhaseTwoReply>(
                  this, METHODID_PHASETWO)))
          .addMethod(
            getLearnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsPaxos.LearnRequest,
                dadkvs.DadkvsPaxos.LearnReply>(
                  this, METHODID_LEARN)))
          .build();
    }
  }

  /**
   */
  public static final class DadkvsPaxosServiceStub extends io.grpc.stub.AbstractAsyncStub<DadkvsPaxosServiceStub> {
    private DadkvsPaxosServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsPaxosServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsPaxosServiceStub(channel, callOptions);
    }

    /**
     */
    public void phaseone(dadkvs.DadkvsPaxos.PhaseOneRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseOneReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPhaseoneMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void phasetwo(dadkvs.DadkvsPaxos.PhaseTwoRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseTwoReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPhasetwoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void learn(dadkvs.DadkvsPaxos.LearnRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.LearnReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLearnMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DadkvsPaxosServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<DadkvsPaxosServiceBlockingStub> {
    private DadkvsPaxosServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsPaxosServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsPaxosServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public dadkvs.DadkvsPaxos.PhaseOneReply phaseone(dadkvs.DadkvsPaxos.PhaseOneRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPhaseoneMethod(), getCallOptions(), request);
    }

    /**
     */
    public dadkvs.DadkvsPaxos.PhaseTwoReply phasetwo(dadkvs.DadkvsPaxos.PhaseTwoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPhasetwoMethod(), getCallOptions(), request);
    }

    /**
     */
    public dadkvs.DadkvsPaxos.LearnReply learn(dadkvs.DadkvsPaxos.LearnRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLearnMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DadkvsPaxosServiceFutureStub extends io.grpc.stub.AbstractFutureStub<DadkvsPaxosServiceFutureStub> {
    private DadkvsPaxosServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsPaxosServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsPaxosServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsPaxos.PhaseOneReply> phaseone(
        dadkvs.DadkvsPaxos.PhaseOneRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPhaseoneMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsPaxos.PhaseTwoReply> phasetwo(
        dadkvs.DadkvsPaxos.PhaseTwoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPhasetwoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsPaxos.LearnReply> learn(
        dadkvs.DadkvsPaxos.LearnRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLearnMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PHASEONE = 0;
  private static final int METHODID_PHASETWO = 1;
  private static final int METHODID_LEARN = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DadkvsPaxosServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DadkvsPaxosServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PHASEONE:
          serviceImpl.phaseone((dadkvs.DadkvsPaxos.PhaseOneRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseOneReply>) responseObserver);
          break;
        case METHODID_PHASETWO:
          serviceImpl.phasetwo((dadkvs.DadkvsPaxos.PhaseTwoRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.PhaseTwoReply>) responseObserver);
          break;
        case METHODID_LEARN:
          serviceImpl.learn((dadkvs.DadkvsPaxos.LearnRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsPaxos.LearnReply>) responseObserver);
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

  private static abstract class DadkvsPaxosServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DadkvsPaxosServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return dadkvs.DadkvsPaxos.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DadkvsPaxosService");
    }
  }

  private static final class DadkvsPaxosServiceFileDescriptorSupplier
      extends DadkvsPaxosServiceBaseDescriptorSupplier {
    DadkvsPaxosServiceFileDescriptorSupplier() {}
  }

  private static final class DadkvsPaxosServiceMethodDescriptorSupplier
      extends DadkvsPaxosServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DadkvsPaxosServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DadkvsPaxosServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DadkvsPaxosServiceFileDescriptorSupplier())
              .addMethod(getPhaseoneMethod())
              .addMethod(getPhasetwoMethod())
              .addMethod(getLearnMethod())
              .build();
        }
      }
    }
    return result;
  }
}
