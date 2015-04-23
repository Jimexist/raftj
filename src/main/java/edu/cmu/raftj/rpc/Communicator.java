package edu.cmu.raftj.rpc;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.AppendEntriesResponse;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;

import java.util.List;

/**
 * Communication module for {@link edu.cmu.raftj.server.Server}
 */
public interface Communicator {

    /**
     * invoked by candidates to gather votes
     *
     * @param voteRequest vote request
     * @return response
     */
    ListenableFuture<List<VoteResponse>> sendVoteRequest(VoteRequest voteRequest);

    /**
     * invoked by leader to replicate log entries and heartbeat
     *
     * @param appendEntriesRequest append request
     * @param follower             follower host and port
     * @return response
     */
    ListenableFuture<AppendEntriesResponse> sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest,
                                                                           HostAndPort follower);

    void setRequestListener(RequestListener requestListener);

    /**
     * @return the host and port for current server
     */
    HostAndPort getServerHostAndPort();

    /**
     * @return the audience of the communicator, i.e. other servers
     */
    ImmutableSet<HostAndPort> getAudience();

}
