package edu.cmu.raftj.server;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.*;
import edu.cmu.raftj.rpc.RequestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static edu.cmu.raftj.server.Server.Role.Candidate;
import static edu.cmu.raftj.server.Server.Role.Leader;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractExecutionThreadService implements Server, RequestListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultServer.class);

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);

    private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());

    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final long electionTimeout;
    private final Communicator communicator;

    private final Map<String, Long> nextIndices = Maps.newConcurrentMap();
    private final Map<String, Long> matchIndices = Maps.newConcurrentMap();

    public DefaultServer(long electionTimeout, Communicator communicator) throws IOException {
        checkArgument(electionTimeout > 0, "election timeout must be positive");
        this.electionTimeout = electionTimeout;
        this.communicator = checkNotNull(communicator, "communicator");
    }

    @Override
    public VoteResponse onVoteRequest(VoteRequest voteRequest) {
        logger.info("vote request {}", voteRequest);

        final long term = voteRequest.getCandidateTerm();
        final long current = currentTerm.get();
        if (term > current && currentTerm.compareAndSet(term, current)) {
            // convert to follower
            currentRole.set(Role.Follower);

        }

        return null;
    }

    @Override
    public AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        logger.info("append entries request {}", appendEntriesRequest);
        lastHeartbeat.set(System.currentTimeMillis());

        AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();

        if (appendEntriesRequest.getLeaderTerm() < currentTerm.get()) {
            builder.setSuccess(false);
            return builder.setTerm(currentTerm.get()).build();
        }

        List<LogEntry> entryList = appendEntriesRequest.getLogEntriesList();

        return null;
    }

    /**
     * leader sends heartbeat
     */
    private void sendHeartbeat() {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(currentTerm.get())
                .setLeaderId(getServerId())
                .setLeaderCommitIndex(commitIndex.get())
                .build();
        communicator.sendAppendEntriesRequest(request);
    }

    private void reinitializeLeaderStates() {
        nextIndices.clear();
        matchIndices.clear();
    }


    /**
     * apply pending commits
     */
    private void applyCommits() {
        while (lastApplied.get() < commitIndex.get()) {
            long index = lastApplied.getAndIncrement();
        }
    }

    /**
     * followers check election timeout
     */
    private void checkElectionTimeout() {
        if (lastHeartbeat.get() + electionTimeout < System.currentTimeMillis()) {
            logger.info("election timeout, convert to candidate");
            currentRole.set(Candidate);
            startElection();
        }
    }

    private void startElection() {
        logger.info("try to start election, current term {}", currentTerm.get());
        while (currentRole.get() == Candidate) {
            long newTerm = currentTerm.incrementAndGet();
            logger.info("incremented to new term {}", newTerm);
            VoteRequest voteRequest = VoteRequest.newBuilder()
                    .setCandidateId(getServerId())
                    .setCandidateTerm(newTerm)
                            // todo
                    .build();

            try {
                Collection<VoteResponse> responses =
                        communicator.sendVoteRequest(voteRequest).get(electionTimeout, TimeUnit.MILLISECONDS);
                long numberOfAyes = responses.stream().filter((vote) -> vote != null && vote.getVoteGranted()).count();
                if (2 * (numberOfAyes + 1) > responses.size()) {
                    if (currentRole.compareAndSet(Candidate, Leader)) {
                        logger.info("won election, current term {}", currentTerm.get());
                        sendHeartbeat();
                        reinitializeLeaderStates();
                        return;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw Throwables.propagate(e);
            } catch (TimeoutException e) {
                logger.info("election timeout, restart election");
            }
        }
        logger.info("lose election, current term {}", currentTerm.get());
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            // apply commits if any pending ones are present
            applyCommits();

            Role role = currentRole.get();
            switch (role) {
                case Follower:
                    checkElectionTimeout();
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

    @Override
    public Role getCurrentRole() {
        return currentRole.get();
    }

    @Override
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    @Override
    public long getElectionTimeout() {
        return electionTimeout;
    }

    @Override
    public String getServerId() {
        return communicator.getServerHostAndPort().toString();
    }
}
