package com.alex.server.model;

import java.io.Serializable;
import java.util.Objects;

public class LogEntry implements Serializable {

    private long term;
    private String command;
    private int index;

    public LogEntry(long term, String command, int index) {
        this.term = term;
        this.command = command;
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                index == logEntry.index &&
                Objects.equals(command, logEntry.command);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", command='" + command + '\'' +
                ", index=" + index +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, command, index);
    }
}
