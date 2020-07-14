package com.alex.streaming.server;

import com.alex.raft.streaming.HelloReply;
import com.alex.raft.streaming.HelloRequest;
import com.alex.raft.streaming.StreamingGreeterGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class StreamingGreeterImpl extends StreamingGreeterGrpc.StreamingGreeterImplBase {
    private final Logger logger;
    private final String serverName;

    public StreamingGreeterImpl(Logger logger, String serverName) {
        this.logger = logger;
        this.serverName = serverName;
    }

    @Override
    public StreamObserver<HelloRequest> sayHelloStreaming(final StreamObserver<HelloReply> responseObserver) {

        final ServerCallStreamObserver<HelloReply> serverCallStreamObserver =
                (ServerCallStreamObserver<HelloReply>) responseObserver;
        serverCallStreamObserver.disableAutoInboundFlowControl();

        class OnReadyHandler implements Runnable {
            private boolean wasReady = false;

            @Override
            public void run() {
                if (serverCallStreamObserver.isReady() && !wasReady) {
                    wasReady = true;
                    logger.info("READY " + serverName);
                    serverCallStreamObserver.request(1);
                }
            }
        }
        final OnReadyHandler onReadyHandler = new OnReadyHandler();
        serverCallStreamObserver.setOnReadyHandler(onReadyHandler);

        // Give gRPC a StreamObserver that can observe and process incoming requests.
        return new StreamObserver<>() {
            @Override
            public void onNext(HelloRequest request) {
                try {
                    String name = request.getName();
                    logger.info("--> " + name);

                    Thread.sleep(100);

                    String message = "Hello " + name + " from " + serverName;
                    logger.info("<-- " + message);
                    HelloReply reply = HelloReply.newBuilder().setMessage(message).build();
                    responseObserver.onNext(reply);

                    if (serverCallStreamObserver.isReady()) {
                        serverCallStreamObserver.request(1);
                    } else {
                        onReadyHandler.wasReady = false;
                    }
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    responseObserver.onError(
                            Status.UNKNOWN.withDescription("Error handling request").withCause(throwable).asException());
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                logger.info("COMPLETED");
                responseObserver.onCompleted();
            }
        };
    }
}
