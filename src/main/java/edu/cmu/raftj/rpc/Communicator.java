package edu.cmu.raftj.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.AppendEntriesResponse;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;

import java.util.Collection;

/**
 * Communication module for {@link edu.cmu.raftj.server.Server}
 */
public interface Communicator {

    ListenableFuture<? extends Collection<VoteResponse>> sendVoteRequest(VoteRequest voteRequest);

    ListenableFuture<? extends Collection<AppendEntriesResponse>> sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest);

    void setRequestListener(RequestListener requestListener);

    String getSenderId();

}
