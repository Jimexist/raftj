package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;

/**
 * Listening to RPC requests
 */
public interface RequestListener {

    void onVoteRequest(VoteRequest voteRequest);

    void onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

}
