package com.alex.server.model;

public class ElectionTimeoutProperties {
    private int electionTimeoutLowerBound;
    private int electionTimeoutUpperBound;

    public ElectionTimeoutProperties(int electionTimeoutLowerBound, int electionTimeoutUpperBound) {
        this.electionTimeoutLowerBound = electionTimeoutLowerBound;
        this.electionTimeoutUpperBound = electionTimeoutUpperBound;
    }

    public int getElectionTimeoutLowerBound() {
        return electionTimeoutLowerBound;
    }

    public int getElectionTimeoutUpperBound() {
        return electionTimeoutUpperBound;
    }

    public void setElectionTimeoutLowerBound(int electionTimeoutLowerBound) {
        this.electionTimeoutLowerBound = electionTimeoutLowerBound;
    }

    public void setElectionTimeoutUpperBound(int electionTimeoutUpperBound) {
        this.electionTimeoutUpperBound = electionTimeoutUpperBound;
    }
}
