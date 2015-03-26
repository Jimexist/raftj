package edu.cmu.raftj.rpc;

import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;

/**
 * Created by jiayu on 3/19/15.
 */
public interface Communicator {

    void sendVoteRequest(VoteRequest voteRequest);

    void sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

    void setRequestListener(RequestListener requestListener);

}
