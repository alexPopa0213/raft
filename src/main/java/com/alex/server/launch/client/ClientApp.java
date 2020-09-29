package com.alex.server.launch.client;

import com.alex.raft.RaftServiceGrpc;
import com.alex.raft.client.ClientReply;
import com.alex.raft.client.ClientRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;

public class ClientApp {

    public static void main(String[] args) {

        int port = 50051;

        List<String> commands = new ArrayList<>();
        commands.add("command1");

        ClientRequest clientRequest = ClientRequest.newBuilder()
                .addAllCommands(commands)
                .build();

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext()
                .build();
        RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(managedChannel);

        ClientReply clientReply = stub.sendCommands(clientRequest);
        System.out.println("Received: " + clientReply.getSuccess());
    }
}
