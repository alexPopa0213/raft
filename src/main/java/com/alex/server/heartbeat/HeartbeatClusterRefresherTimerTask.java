package com.alex.server.heartbeat;

import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatClusterRefresherTimerTask extends TimerTask {

    private static final Logger LOGGER = getLogger(HeartbeatClusterRefresherTimerTask.class);

    private final Map<Integer, Timestamp> cluster;

    public HeartbeatClusterRefresherTimerTask(Map<Integer, Timestamp> cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        cluster.entrySet().removeIf(entry -> {
            Timestamp clusterTimestamp = entry.getValue();
            Timestamp currentTimestamp = new Timestamp(new Date().getTime());
            return clusterTimestamp.getTime() < currentTimestamp.getTime() - TimeUnit.SECONDS.toMillis(3);
        });
        LOGGER.trace("Cluster is now: {}", cluster);
    }

}
