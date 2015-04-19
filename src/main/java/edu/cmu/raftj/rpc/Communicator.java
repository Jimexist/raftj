package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;

/**
 * Communication module for {@link edu.cmu.raftj.server.Server}
 */
public interface Communicator {

    void sendVoteRequest(VoteRequest voteRequest);

    void sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

    void setRequestListener(RequestListener requestListener);

}
