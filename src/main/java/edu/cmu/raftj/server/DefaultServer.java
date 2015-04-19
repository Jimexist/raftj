package edu.cmu.raftj.server;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.RequestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractExecutionThreadService implements Server, RequestListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultServer.class);

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
    public void onVoteRequest(VoteRequest voteRequest) {
        logger.info("vote request {}", voteRequest);
    }

    @Override
    public void onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        logger.info("append entries request {}", appendEntriesRequest);
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            // wait for timeout
        }
    }
}
