package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.*;

/**
 * Listening to RPC requests
 */
public interface RequestListener {

    VoteResponse onVoteRequest(VoteRequest voteRequest);

    AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

    ClientMessageResponse onClientCommand(String command);

}
