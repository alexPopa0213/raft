package com.alex.streaming.client;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class ClientResponseObserverImpl<HelloRequest extends com.alex.raft.streaming.HelloRequest, HelloReply extends com.alex.raft.streaming.HelloReply>
        implements ClientResponseObserver<HelloRequest, HelloReply> {

    ClientCallStreamObserver<HelloRequest> requestStream;

    private final Logger logger;
    private final List<String> names;
    private final CountDownLatch countDownLatch;

    public ClientResponseObserverImpl(Logger logger, List<String> names, CountDownLatch countDownLatch) {
        this.logger = logger;
        this.names = new ArrayList<>(names);
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<HelloRequest> requestStream) {
        this.requestStream = requestStream;
        requestStream.disableAutoInboundFlowControl();

        requestStream.setOnReadyHandler(new Runnable() {
            final Iterator<String> iterator = names.iterator();

            @Override
            public void run() {
                // Start generating values from where we left off on a non-gRPC thread.
                while (requestStream.isReady()) {
                    if (iterator.hasNext()) {
                        // Send more messages if there are more messages to send.
                        String name = iterator.next();
                        logger.info("--> " + name);
                        HelloRequest request = (HelloRequest) HelloRequest.newBuilder().setName(name).build();
                        requestStream.onNext(request);
                    } else {
                        requestStream.onCompleted();
                    }
                }
            }
        });
    }

    @Override
    public void onNext(com.alex.raft.streaming.HelloReply value) {
        logger.info("<-- " + value.getMessage());
        requestStream.request(1);
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
        countDownLatch.countDown();
    }

    @Override
    public void onCompleted() {
        logger.info("All Done");
        countDownLatch.countDown();
    }
}