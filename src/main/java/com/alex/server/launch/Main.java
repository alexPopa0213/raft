package com.alex.server.launch;

import com.alex.server.RaftServer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        assert args.length >= 3 : "Not enough arguments received!";
        int serverPort = Integer.parseInt(args[0]);
        String serverId = args[1];
        int restPort = Integer.parseInt(args[2]);
        final RaftServer server1 = new RaftServer(serverPort, restPort, serverId);
        server1.start();
        server1.blockUntilShutdown();
    }
}
