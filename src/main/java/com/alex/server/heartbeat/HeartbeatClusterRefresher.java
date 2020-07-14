package com.alex.server.heartbeat;

import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Simple thread removing the servers from cluster, with a heartbeat older than x seconds;
 */
public class HeartbeatClusterRefresher implements Runnable {
    private static final Logger LOGGER = getLogger(HeartbeatClusterRefresher.class);

    private final Map<Integer, Timestamp> cluster;

    public HeartbeatClusterRefresher(Map<Integer, Timestamp> cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        cluster.entrySet().removeIf(entry -> {
            Timestamp clusterTimestamp = entry.getValue();
            Timestamp currentTimestamp = new Timestamp(new Date().getTime());
            return clusterTimestamp.getTime() < currentTimestamp.getTime() - TimeUnit.SECONDS.toMillis(5);
        });
        LOGGER.trace("Cluster is now: {}", cluster);
    }
}
