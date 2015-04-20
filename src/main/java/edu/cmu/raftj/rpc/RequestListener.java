package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.AppendEntriesResponse;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;

/**
 * Listening to RPC requests
 */
public interface RequestListener {

    VoteResponse onVoteRequest(VoteRequest voteRequest);

    AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

}
