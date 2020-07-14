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

package com.alex.streaming.server;

import com.alex.raft.streaming.HelloReply;
import com.alex.raft.streaming.HelloRequest;
import com.alex.raft.streaming.StreamingGreeterGrpc;
import com.alex.streaming.client.ClientResponseObserverImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.util.Collections.*;

public class ManualFlowControlServer {
    private static final Logger logger =
            Logger.getLogger(ManualFlowControlServer.class.getName());

    public static void main(String[] args) throws InterruptedException, IOException {

        StreamingGreeterImpl streamingGreeter = new StreamingGreeterImpl(logger, "server1");

        final Server server = ServerBuilder
                .forPort(50051)
                .addService(streamingGreeter)
                .build()
                .start();

        logger.info("Listening on " + server.getPort());

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50052)
                .usePlaintext()
                .build();
        StreamingGreeterGrpc.StreamingGreeterStub stub = StreamingGreeterGrpc.newStub(channel);
        ClientResponseObserverImpl<HelloRequest, HelloReply> clientResponseObserver = new ClientResponseObserverImpl<>(logger, singletonList("alexpopa"), new CountDownLatch(1));
        stub.sayHelloStreaming(clientResponseObserver);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("Shutting down server1");
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));

        channel.shutdown();
        channel.awaitTermination(1, TimeUnit.SECONDS);
        server.awaitTermination();
    }

}
