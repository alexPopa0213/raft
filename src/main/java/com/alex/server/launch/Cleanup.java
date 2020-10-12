package com.alex.server.launch;

import com.alex.server.model.LogEntry;
import com.alex.server.model.LogEntrySerializer;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.IndexTreeList;

import java.util.Arrays;
import java.util.List;

public class Cleanup {

    public static void main(String[] args) {
       cleanPersistedData();
    }

    private static void cleanPersistedData() {
        List<String> serversToClean = Arrays.asList("srv1", "srv2, srv3");
        for (String server : serversToClean) {
            String dbName = "db_" + server;
            DB db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
            IndexTreeList<LogEntry> logEntries = db.indexTreeList(server + "_log", new LogEntrySerializer()).createOrOpen();
            logEntries.clear();
            clearTerm(server, db);
            db.close();
        }
    }

    private static void clearTerm(String server, DB db) {
        Atomic.Long term = db.atomicLong(server + "_current_term").createOrOpen();
        term.set(0);
    }
}
