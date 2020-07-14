/*
 * Copyright 2017 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alex.streaming.client;

import com.alex.raft.streaming.HelloReply;
import com.alex.raft.streaming.HelloRequest;
import com.alex.raft.streaming.StreamingGreeterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ManualFlowControlClient {
    private static final Logger LOGGER =
            Logger.getLogger(ManualFlowControlClient.class.getName());

    public static void main(String[] args) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // Create a channel and a stub
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()
                .build();

//        ManagedChannel channel2 = ManagedChannelBuilder
//                .forAddress("localhost", 50052)
//                .usePlaintext()
//                .build();

        StreamingGreeterGrpc.StreamingGreeterStub stub = StreamingGreeterGrpc.newStub(channel);
//        StreamingGreeterGrpc.StreamingGreeterStub stub2 = StreamingGreeterGrpc.newStub(channel2);

        // When using manual flow-control and back-pressure on the client, the ClientResponseObserver handles both
        // request and response streams.

        ClientResponseObserverImpl<HelloRequest, HelloReply> clientResponseObserver = new ClientResponseObserverImpl<>(LOGGER, names(), countDownLatch);
//        ClientResponseObserverImpl<HelloRequest, HelloReply> clientResponseObserver2 = new ClientResponseObserverImpl<>(LOGGER, names(), countDownLatch);

        // Note: clientResponseObserver is handling both request and response stream processing.
        stub.sayHelloStreaming(clientResponseObserver);
//        stub2.sayHelloStreaming(clientResponseObserver2);

        countDownLatch.await();

        channel.shutdown();
        channel.awaitTermination(1, TimeUnit.SECONDS);
//        channel2.shutdown();
//        channel2.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static List<String> names() {
        return Arrays.asList(
                "Sophia",
                "Jackson",
                "Emma",
                "Aiden",
                "Olivia",
                "Lucas",
                "Ava"
        );
    }
}
