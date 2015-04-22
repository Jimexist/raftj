package edu.cmu.raftj.server;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.*;
import edu.cmu.raftj.rpc.RequestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static edu.cmu.raftj.server.Server.Role.Candidate;
import static edu.cmu.raftj.server.Server.Role.Follower;
import static edu.cmu.raftj.server.Server.Role.Leader;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractExecutionThreadService implements Server, RequestListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultServer.class);

    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);

    private final BlockingDeque<Long> heartbeats = Queues.newLinkedBlockingDeque();

    private final AtomicReference<Role> currentRole = new AtomicReference<>(Follower);
    private final long electionTimeout;
    private final Communicator communicator;
    private final Persistence persistence;

    private final Map<String, Long> nextIndices = Maps.newConcurrentMap();
    private final Map<String, Long> matchIndices = Maps.newConcurrentMap();

    public DefaultServer(long electionTimeout, Communicator communicator, Persistence persistence) throws IOException {
        this.electionTimeout = electionTimeout;
        this.communicator = checkNotNull(communicator, "communicator");
        this.persistence = checkNotNull(persistence, "persistence");
    }

    /**
     * sync current term with a potential larger term
     *
     * @param term term
     */
    private void syncCurrentTerm(long term) {
        if (persistence.largerThanAndSetCurrentTerm(term)) {
            logger.info("bump current term to be {}", term);
            currentRole.set(Follower);
        }
    }

    @Override
    public VoteResponse onVoteRequest(VoteRequest voteRequest) {
        logger.info("vote request {}", voteRequest);
        syncCurrentTerm(voteRequest.getCandidateTerm());

        return null;
    }

    @Override
    public AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        logger.info("append entries request {}", appendEntriesRequest);
        syncCurrentTerm(appendEntriesRequest.getLeaderTerm());
        heartbeats.addLast(System.currentTimeMillis());

        AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();

        if (appendEntriesRequest.getLeaderTerm() < persistence.getCurrentTerm()) {
            builder.setSuccess(false);
            return builder.setTerm(persistence.getCurrentTerm()).build();
        }

        List<LogEntry> entryList = appendEntriesRequest.getLogEntriesList();

        return null;
    }

    /**
     * leader sends heartbeat
     */
    private void sendHeartbeat() {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(persistence.getCurrentTerm())
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
            LogEntry logEntry = persistence.getLogEntry(index);
            String command = logEntry.getCommand();
            applyCommand(command);
        }
    }

    /**
     * apply command to the state machine
     *
     * @param command command
     */
    private void applyCommand(String command) {
        logger.info("applying command '{}'", command);
    }

    /**
     * followers check election timeout, block for up to election timeout millis if not available
     */
    private void checkElectionTimeout() {
        try {
            while (true) {
                long hb = heartbeats.poll(electionTimeout, TimeUnit.MILLISECONDS);
                if (hb + electionTimeout > System.currentTimeMillis()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            logger.info("election timeout, convert to candidate");
            currentRole.set(Candidate);
            startElection();
        }
    }

    private void startElection() {
        logger.info("try to start election, current term {}", persistence.getCurrentTerm());
        while (currentRole.get() == Candidate) {
            long newTerm = persistence.incrementAndGetCurrentTerm();
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
                        logger.info("won election, current term {}", persistence.getCurrentTerm());
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
        logger.info("lose election, current term {}", persistence.getCurrentTerm());
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
        return persistence.getCurrentTerm();
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
