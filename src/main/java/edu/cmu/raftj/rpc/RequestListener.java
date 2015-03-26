package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;

/**
 * Created by jiayu on 3/26/15.
 */
public interface RequestListener {

    void onVoteRequest(VoteRequest voteRequest);

    void onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

}
