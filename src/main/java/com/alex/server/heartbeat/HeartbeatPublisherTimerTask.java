package com.alex.server.heartbeat;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.TimerTask;

import static java.lang.Runtime.getRuntime;
import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatPublisherTimerTask extends TimerTask {

    private static final Logger LOGGER = getLogger(HeartbeatPublisherTimerTask.class);

    private DatagramSocket socket = null;
    private InetAddress group = null;
    private byte[] buf;

    private final Integer port;

    public HeartbeatPublisherTimerTask(Integer port) {
        this.port = port;
        initSocket();
        addShutdownHook();
    }

    @Override
    public void run() {
        try {
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, 4466);
            socket.send(packet);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void initSocket() {
        try {
            socket = new DatagramSocket();
            group = InetAddress.getByName("230.0.0.0");
            buf = port.toString().getBytes();
        } catch (SocketException | UnknownHostException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void addShutdownHook() {
        getRuntime().addShutdownHook(new Thread(() -> {
            socket.close();
        }));
    }
}
