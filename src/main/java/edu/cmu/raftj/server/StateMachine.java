package edu.cmu.raftj.server;

import edu.cmu.raftj.persistence.Persistence;

/**
 * state machine, should have no hard states, all pulled from the {@link Persistence}
 */
public interface StateMachine {

    long getCommitIndex();

    void increaseCommitIndex(long possibleLargerCommitIndex);

    void applyAllPendingCommandsFrom(Persistence persistence);

}

