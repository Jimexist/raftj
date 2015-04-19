package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractExecutionThreadService implements Server, RequestListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultServer.class);

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final ListeningScheduledExecutorService timeoutExecutor =
            listeningDecorator(newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TimeoutListener").build()));
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

        final long term = voteRequest.getCandidateTerm();
        final long current = currentTerm.get();
        if (term > current && currentTerm.compareAndSet(term, current)) {
            // convert to follower
            currentRole.set(Role.Follower);

        }
    }

    @Override
    public void onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        logger.info("append entries request {}", appendEntriesRequest);
    }

    private void sendHeartbeat() {
        checkState(currentRole.get() == Role.Leader, "only leader can send heartbeats");
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(currentTerm.get())

                .addAllLogEntries(ImmutableList.of())
                .build();
        communicator.sendAppendEntriesRequest(request);
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            Role role = currentRole.get();
            switch (role) {
                case Follower:
                    break;
                case Leader:
                    sendHeartbeat();
                    break;
                case Candidate:
                    break;
                default:
                    throw new IllegalStateException("invalid role: " + role);
            }
        }
    }
}
