package com.alex.server.unit;

import com.alex.server.model.LogEntry;
import com.alex.server.serdes.ProtoBuilderUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;

public class ProtoBuilderUtilTest {

    @Test
    public void test_convertLogEntryToProtoLogEntry() {
        LogEntry logEntry = new LogEntry(1L, "command1", 0);
        LogEntry logEntry2 = new LogEntry(2L, "command2", 1);
        List<com.alex.raft.LogEntry> protoEntries = ProtoBuilderUtil.toProto(asList(logEntry, logEntry2));

        com.alex.raft.LogEntry protoEntry = buildExpectedProtoEntry(1L, "command1", 0);
        com.alex.raft.LogEntry protoEntry2 = buildExpectedProtoEntry(2L, "command2", 1);

        Assert.assertEquals(2, protoEntries.size());
        Assert.assertEquals(asList(protoEntry, protoEntry2), protoEntries);
    }

    private com.alex.raft.LogEntry buildExpectedProtoEntry(Long term, String command, int index) {
        return com.alex.raft.LogEntry.newBuilder()
                .setTerm(term)
                .setCommand(command)
                .setIndex(index)
                .build();
    }
}
