package com.alex.server.unit;

import com.alex.server.model.LogEntry;
import com.alex.server.util.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class UtilsUnitTest {

    @Test
    public void test_add_missing_entries() {
        LogEntry entry1 = new LogEntry(1L, "command1", 0);
        LogEntry entry2 = new LogEntry(1L, "command2", 1);
        LogEntry entry3 = new LogEntry(2L, "command3", 2);
        LogEntry entry4 = new LogEntry(3L, "command4", 3);
        List<LogEntry> logEntries = new ArrayList<>(Arrays.asList(entry1, entry2, entry3, entry4));

        com.alex.raft.LogEntry newEntry1 = createEntry(3L, "commandX", 4);
        com.alex.raft.LogEntry newEntry2 = createEntry(4L, "commandY", 5);
        com.alex.raft.LogEntry newEntry3 = createEntry(4L, "commandZ", 6);
        com.alex.raft.LogEntry newEntry4 = createEntry(4L, "commandW", 7);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(asList(newEntry1, newEntry2, newEntry3, newEntry4));

        logEntries.addAll(Utils.findMissingEntries(logEntries, newEntries));

        Assert.assertEquals(8, logEntries.size());
    }

    @Test
    public void test_add_missing_entries_withVoidValueOnly() {
        LogEntry entry1 = new LogEntry(0, "", 0);
        List<LogEntry> logEntries = new ArrayList<>(Collections.singletonList(entry1));
        com.alex.raft.LogEntry newEntry1 = createEntry(3L, "commandX", 1);
        com.alex.raft.LogEntry newEntry2 = createEntry(4L, "commandX", 2);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(Arrays.asList(newEntry1, newEntry2));
        logEntries.addAll(Utils.findMissingEntries(logEntries, newEntries));

        Assert.assertEquals(3, logEntries.size());
    }

    @Test
    public void test_add_missing_entries_when_list_is_Empty() {
        List<LogEntry> logEntries = new ArrayList<>();
        com.alex.raft.LogEntry newEntry1 = createEntry(3L, "commandX", 4);
        com.alex.raft.LogEntry newEntry2 = createEntry(4L, "commandY", 5);
        com.alex.raft.LogEntry newEntry3 = createEntry(4L, "commandZ", 6);
        com.alex.raft.LogEntry newEntry4 = createEntry(4L, "commandW", 7);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(asList(newEntry1, newEntry2, newEntry3, newEntry4));

        List<LogEntry> missingEntries = Utils.findMissingEntries(logEntries, newEntries);

        Assert.assertEquals(4, missingEntries.size());
    }

    @Test
    public void test_remove_conflicting_entries() {
        LogEntry entry1 = new LogEntry(1L, "command1", 0);
        LogEntry entry2 = new LogEntry(1L, "command2", 1);
        LogEntry entry3 = new LogEntry(2L, "command3", 2);
        LogEntry entry4 = new LogEntry(3L, "command4", 3);
        LogEntry entry5 = new LogEntry(3L, "command5", 4);
        List<LogEntry> logEntries = new ArrayList<>(asList(entry1, entry2, entry3, entry4, entry5));

        com.alex.raft.LogEntry newEntry1 = createEntry(4L, "commandX", 3);
        com.alex.raft.LogEntry newEntry2 = createEntry(5L, "commandY", 4);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(asList(newEntry1, newEntry2));

        List<LogEntry> sanitized = Utils.removeConflictingAndExtraEntries(logEntries, newEntries, 0);
        Assert.assertEquals(3, sanitized.size());
    }

    @Test
    public void test_remove_conflicting_entries_And_Add_Missing() {
        LogEntry entry1 = new LogEntry(1L, "command1", 0);
        LogEntry entry2 = new LogEntry(1L, "command2", 1);
        LogEntry entry3 = new LogEntry(2L, "command3", 2);
        LogEntry entry4 = new LogEntry(3L, "command4", 3);
        LogEntry entry5 = new LogEntry(3L, "command5", 4);
        List<LogEntry> logEntries = new ArrayList<>(asList(entry1, entry2, entry3, entry4, entry5));

        com.alex.raft.LogEntry newEntry1 = createEntry(4L, "commandX", 3);
        com.alex.raft.LogEntry newEntry2 = createEntry(5L, "commandY", 4);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(asList(newEntry1, newEntry2));

        List<LogEntry> sanitized = Utils.removeConflictingAndExtraEntries(logEntries, newEntries, 0);
        sanitized.addAll(Utils.findMissingEntries(sanitized, newEntries));
        Assert.assertEquals(5, sanitized.size());
    }

    @Test
    public void test_remove_extra_entries() {
        LogEntry entry1 = new LogEntry(1L, "command1", 0);
        LogEntry entry2 = new LogEntry(1L, "command2", 1);
        LogEntry entry3 = new LogEntry(2L, "command3", 2);
        LogEntry entry4 = new LogEntry(3L, "command4", 3);
        LogEntry entry5 = new LogEntry(3L, "command5", 4);
        LogEntry entry6 = new LogEntry(3L, "command6", 5);
        LogEntry entry7 = new LogEntry(3L, "command7", 6);
        List<LogEntry> logEntries = new ArrayList<>(asList(entry1, entry2, entry3, entry4, entry5, entry6, entry7));

        com.alex.raft.LogEntry newEntry1 = createEntry(3L, "commandX", 3);
        com.alex.raft.LogEntry newEntry2 = createEntry(3L, "commandY", 4);

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>(asList(newEntry1, newEntry2));

        List<LogEntry> sanitized = Utils.removeConflictingAndExtraEntries(logEntries, newEntries, 0);
        Assert.assertEquals(5, sanitized.size());
    }

    @Test
    public void test_remove_extra_entries_When_ReceivingEmptyRPC() {
        LogEntry entry1 = new LogEntry(1L, "command1", 0);
        LogEntry entry2 = new LogEntry(1L, "command2", 1);
        LogEntry entry3 = new LogEntry(2L, "command3", 2);
        LogEntry entry4 = new LogEntry(3L, "command4", 3);
        LogEntry entry5 = new LogEntry(3L, "command5", 4);
        LogEntry entry6 = new LogEntry(3L, "command6", 5);
        LogEntry entry7 = new LogEntry(3L, "command7", 6);
        List<LogEntry> logEntries = new ArrayList<>(asList(entry1, entry2, entry3, entry4, entry5, entry6, entry7));

        List<com.alex.raft.LogEntry> newEntries = new ArrayList<>();

        List<LogEntry> sanitized = Utils.removeConflictingAndExtraEntries(logEntries, newEntries, 4);
        Assert.assertEquals(5, sanitized.size());
    }

    private com.alex.raft.LogEntry createEntry(long term, String command, int index) {
        return com.alex.raft.LogEntry.newBuilder()
                .setTerm(term)
                .setCommand(command)
                .setIndex(index)
                .build();
    }
}
