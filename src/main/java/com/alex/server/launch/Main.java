package com.alex.server.launch;

import com.alex.server.RaftServer;
import com.alex.server.model.LogEntry;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static org.apache.logging.log4j.LogManager.getLogger;

public class Main {
    private static final Logger LOGGER = getLogger(Main.class);

    private static final String PRECONDITION_FILE_PREFIX = "precondition_";
    private static final String PRECONDITION_FILE_TYPE = ".txt";
    private static final String PRECONDITION_LOCATION = "/preconditions/";

    public static void main(String[] args) throws InterruptedException {
        assert args.length >= 3 : "Not enough arguments received!";
        int serverPort = parseInt(args[0]);
        String serverId = args[1];
        int restPort = parseInt(args[2]);
        List<LogEntry> precondition = new ArrayList<>();
        int preconditionNumber;
        if (args.length == 4) {
            preconditionNumber = parseInt(args[3]);
            precondition = readPrecondition(preconditionNumber);
            if (!precondition.isEmpty()) {
                LOGGER.debug("Starting with precondition {}.", preconditionNumber);
            }
        }
        final RaftServer server1 = new RaftServer(serverPort, restPort, serverId, precondition);
        server1.start();
        server1.blockUntilShutdown();
    }

    /**
     * Just to be able to simulate some scenarios like: leader failed without replicating entries
     * Followers have missing or/and extra entries
     */
    private static List<com.alex.server.model.LogEntry> readPrecondition(int preconditionNumber) {
        String preconditionFile = PRECONDITION_FILE_PREFIX + preconditionNumber + PRECONDITION_FILE_TYPE;
        List<com.alex.server.model.LogEntry> entries = new ArrayList<>();
        try {
            InputStream inputStream = Main.class.getResourceAsStream(PRECONDITION_LOCATION + preconditionFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");
                entries.add(new LogEntry(parseLong(split[0]), split[1], parseInt(split[2])));
            }
        } catch (IOException ex) {
            LOGGER.warn("Could not read precondition. Starting fresh.", ex);
        }
        return entries;
    }
}
