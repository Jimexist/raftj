package edu.cmu.raftj.server;

/**
 * Server is the common abstraction for all types of running instances / participants.
 */
public interface Server {

    enum Role {
        Leader,
        Follower,
        Candidate
    }

}
