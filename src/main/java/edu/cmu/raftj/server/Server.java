package edu.cmu.raftj.server;

/**
 * Created by jiayu on 3/19/15.
 */
public interface Server {

    enum Role {
        Leader,
        Follower,
        Candidate
    }

}
