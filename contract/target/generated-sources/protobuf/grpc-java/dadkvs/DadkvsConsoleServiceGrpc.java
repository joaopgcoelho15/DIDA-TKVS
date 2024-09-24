package dadkvs;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.36.0)",
    comments = "Source: DadkvsConsole.proto")
public final class DadkvsConsoleServiceGrpc {

  private DadkvsConsoleServiceGrpc() {}

  public static final String SERVICE_NAME = "dadkvs.DadkvsConsoleService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetLeaderRequest,
      dadkvs.DadkvsConsole.SetLeaderReply> getSetleaderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "setleader",
      requestType = dadkvs.DadkvsConsole.SetLeaderRequest.class,
      responseType = dadkvs.DadkvsConsole.SetLeaderReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetLeaderRequest,
      dadkvs.DadkvsConsole.SetLeaderReply> getSetleaderMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetLeaderRequest, dadkvs.DadkvsConsole.SetLeaderReply> getSetleaderMethod;
    if ((getSetleaderMethod = DadkvsConsoleServiceGrpc.getSetleaderMethod) == null) {
      synchronized (DadkvsConsoleServiceGrpc.class) {
        if ((getSetleaderMethod = DadkvsConsoleServiceGrpc.getSetleaderMethod) == null) {
          DadkvsConsoleServiceGrpc.getSetleaderMethod = getSetleaderMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsConsole.SetLeaderRequest, dadkvs.DadkvsConsole.SetLeaderReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "setleader"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsConsole.SetLeaderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsConsole.SetLeaderReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsConsoleServiceMethodDescriptorSupplier("setleader"))
              .build();
        }
      }
    }
    return getSetleaderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetDebugRequest,
      dadkvs.DadkvsConsole.SetDebugReply> getSetdebugMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "setdebug",
      requestType = dadkvs.DadkvsConsole.SetDebugRequest.class,
      responseType = dadkvs.DadkvsConsole.SetDebugReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetDebugRequest,
      dadkvs.DadkvsConsole.SetDebugReply> getSetdebugMethod() {
    io.grpc.MethodDescriptor<dadkvs.DadkvsConsole.SetDebugRequest, dadkvs.DadkvsConsole.SetDebugReply> getSetdebugMethod;
    if ((getSetdebugMethod = DadkvsConsoleServiceGrpc.getSetdebugMethod) == null) {
      synchronized (DadkvsConsoleServiceGrpc.class) {
        if ((getSetdebugMethod = DadkvsConsoleServiceGrpc.getSetdebugMethod) == null) {
          DadkvsConsoleServiceGrpc.getSetdebugMethod = getSetdebugMethod =
              io.grpc.MethodDescriptor.<dadkvs.DadkvsConsole.SetDebugRequest, dadkvs.DadkvsConsole.SetDebugReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "setdebug"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsConsole.SetDebugRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  dadkvs.DadkvsConsole.SetDebugReply.getDefaultInstance()))
              .setSchemaDescriptor(new DadkvsConsoleServiceMethodDescriptorSupplier("setdebug"))
              .build();
        }
      }
    }
    return getSetdebugMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DadkvsConsoleServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceStub>() {
        @java.lang.Override
        public DadkvsConsoleServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsConsoleServiceStub(channel, callOptions);
        }
      };
    return DadkvsConsoleServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DadkvsConsoleServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceBlockingStub>() {
        @java.lang.Override
        public DadkvsConsoleServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsConsoleServiceBlockingStub(channel, callOptions);
        }
      };
    return DadkvsConsoleServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DadkvsConsoleServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DadkvsConsoleServiceFutureStub>() {
        @java.lang.Override
        public DadkvsConsoleServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DadkvsConsoleServiceFutureStub(channel, callOptions);
        }
      };
    return DadkvsConsoleServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class DadkvsConsoleServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void setleader(dadkvs.DadkvsConsole.SetLeaderRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetLeaderReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetleaderMethod(), responseObserver);
    }

    /**
     */
    public void setdebug(dadkvs.DadkvsConsole.SetDebugRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetDebugReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetdebugMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSetleaderMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsConsole.SetLeaderRequest,
                dadkvs.DadkvsConsole.SetLeaderReply>(
                  this, METHODID_SETLEADER)))
          .addMethod(
            getSetdebugMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                dadkvs.DadkvsConsole.SetDebugRequest,
                dadkvs.DadkvsConsole.SetDebugReply>(
                  this, METHODID_SETDEBUG)))
          .build();
    }
  }

  /**
   */
  public static final class DadkvsConsoleServiceStub extends io.grpc.stub.AbstractAsyncStub<DadkvsConsoleServiceStub> {
    private DadkvsConsoleServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsConsoleServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsConsoleServiceStub(channel, callOptions);
    }

    /**
     */
    public void setleader(dadkvs.DadkvsConsole.SetLeaderRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetLeaderReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetleaderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setdebug(dadkvs.DadkvsConsole.SetDebugRequest request,
        io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetDebugReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetdebugMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DadkvsConsoleServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<DadkvsConsoleServiceBlockingStub> {
    private DadkvsConsoleServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsConsoleServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsConsoleServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public dadkvs.DadkvsConsole.SetLeaderReply setleader(dadkvs.DadkvsConsole.SetLeaderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetleaderMethod(), getCallOptions(), request);
    }

    /**
     */
    public dadkvs.DadkvsConsole.SetDebugReply setdebug(dadkvs.DadkvsConsole.SetDebugRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetdebugMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DadkvsConsoleServiceFutureStub extends io.grpc.stub.AbstractFutureStub<DadkvsConsoleServiceFutureStub> {
    private DadkvsConsoleServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DadkvsConsoleServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DadkvsConsoleServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsConsole.SetLeaderReply> setleader(
        dadkvs.DadkvsConsole.SetLeaderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetleaderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dadkvs.DadkvsConsole.SetDebugReply> setdebug(
        dadkvs.DadkvsConsole.SetDebugRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetdebugMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SETLEADER = 0;
  private static final int METHODID_SETDEBUG = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DadkvsConsoleServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DadkvsConsoleServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SETLEADER:
          serviceImpl.setleader((dadkvs.DadkvsConsole.SetLeaderRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetLeaderReply>) responseObserver);
          break;
        case METHODID_SETDEBUG:
          serviceImpl.setdebug((dadkvs.DadkvsConsole.SetDebugRequest) request,
              (io.grpc.stub.StreamObserver<dadkvs.DadkvsConsole.SetDebugReply>) responseObserver);
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

  private static abstract class DadkvsConsoleServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DadkvsConsoleServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return dadkvs.DadkvsConsole.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DadkvsConsoleService");
    }
  }

  private static final class DadkvsConsoleServiceFileDescriptorSupplier
      extends DadkvsConsoleServiceBaseDescriptorSupplier {
    DadkvsConsoleServiceFileDescriptorSupplier() {}
  }

  private static final class DadkvsConsoleServiceMethodDescriptorSupplier
      extends DadkvsConsoleServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DadkvsConsoleServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (DadkvsConsoleServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DadkvsConsoleServiceFileDescriptorSupplier())
              .addMethod(getSetleaderMethod())
              .addMethod(getSetdebugMethod())
              .build();
        }
      }
    }
    return result;
  }
}
