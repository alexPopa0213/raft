package com.alex.server.heartbeat;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatPublisher implements Runnable {
    private static final Logger LOGGER = getLogger(HeartbeatPublisher.class);

    private final Integer port;

    public HeartbeatPublisher(Integer port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            DatagramSocket socket = new DatagramSocket();
            InetAddress group = InetAddress.getByName("230.0.0.0");
            byte[] buf = port.toString().getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, 4466);
            socket.send(packet);
            socket.close();
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }
}
