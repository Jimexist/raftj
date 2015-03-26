package edu.cmu.raftj.server;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages;
import edu.cmu.raftj.rpc.RequestListener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jiayu on 3/19/15.
 */
public class DefaultServer extends AbstractExecutionThreadService implements Server, RequestListener {

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final long electionTimeout;
    private final Communicator communicator;

    public DefaultServer(long electionTimeout, Communicator communicator) throws IOException {
        checkArgument(electionTimeout > 0, "election timeout must be positive");
        this.electionTimeout = electionTimeout;
        this.communicator = checkNotNull(communicator, "communicator");
    }

    @Override
    public void onVoteRequest(Messages.VoteRequest voteRequest) {

    }

    @Override
    public void onAppendEntriesRequest(Messages.AppendEntriesRequest appendEntriesRequest) {

    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            // wait for timeout
        }
    }
}
