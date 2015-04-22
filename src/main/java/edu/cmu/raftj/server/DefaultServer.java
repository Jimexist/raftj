package edu.cmu.raftj.server;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.*;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.*;
import edu.cmu.raftj.rpc.RequestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static edu.cmu.raftj.server.Server.Role.*;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractScheduledService implements Server, RequestListener {

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
            logger.info("[{}] bump current term to be {}", currentRole.get(), term);
            currentRole.set(Follower);
        }
    }

    @Override
    public VoteResponse onVoteRequest(VoteRequest voteRequest) {
        logger.info("[{}] got vote request {}", currentRole.get(), voteRequest);
        syncCurrentTerm(voteRequest.getCandidateTerm());

        final VoteResponse.Builder builder = VoteResponse.newBuilder().setVoteGranted(false);
        final String candidateId = checkNotNull(voteRequest.getCandidateId(), "candidate ID");
        final boolean upToDate = voteRequest.getLastLogIndex() >= (persistence.getLogEntriesSize() + 1);
        if (voteRequest.getCandidateTerm() >= getCurrentTerm() && upToDate) {
            if (Objects.equals(candidateId, persistence.getVotedFor()) ||
                    persistence.compareAndSetVoteFor(null, candidateId)) {
                logger.info("[{}] voted yes for {}", currentRole.get(), candidateId);
                builder.setVoteGranted(true);
            } else {
                logger.info("[{}] voted no for {} because already voted for {}",
                        currentRole.get(), candidateId, persistence.getVotedFor());
            }
        } else {
            logger.info("[{}] voted no for {} because the request was too old",
                    currentRole.get(), candidateId);
        }
        return builder.setTerm(getCurrentTerm()).build();
    }

    private void updateCommitIndex(AppendEntriesRequest request) {
        commitIndex.updateAndGet(value -> {
            final long leaderCommitIndex = request.getLeaderCommitIndex();
            if (leaderCommitIndex > value) {
                final List<LogEntry> entryList = request.getLogEntriesList();
                if (!entryList.isEmpty()) {
                    return Math.min(leaderCommitIndex, entryList.get(entryList.size() - 1).getLogIndex());
                }
                return leaderCommitIndex;
            } else {
                return value;
            }
        });
    }

    @Override
    public AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest request) {
        logger.info("[{}] append entries request {}", currentRole.get(), request);
        syncCurrentTerm(request.getLeaderTerm());
        heartbeats.addLast(System.currentTimeMillis());

        final AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder().setSuccess(false);
        // the term is new
        if (request.getLeaderTerm() >= getCurrentTerm()) {
            final LogEntry lastEntry = persistence.getLastLogEntry();
            if (lastEntry != null && lastEntry.getLogIndex() >= request.getPrevLogIndex()) {
                final LogEntry entry = persistence.getLogEntry(request.getPrevLogIndex());
                if (entry.getTerm() == request.getPrevLogTerm()) {
                    request.getLogEntriesList().stream().forEach(persistence::applyLogEntry);
                    updateCommitIndex(request);
                    builder.setSuccess(true);
                } else {
                    logger.warn("[{}] prev entry mismatch, local last term {}, remote prev term {}",
                            currentRole.get(), entry.getTerm(), request.getPrevLogTerm());
                }
            } else {
                logger.warn("[{}] log entries are too new, remote prev index {}",
                        currentRole.get(),
                        request.getPrevLogIndex());
            }
        }
        return builder.setTerm(getCurrentTerm()).build();
    }

    /**
     * leader sends heartbeat
     */
    private void sendHeartbeat() {
        logger.info("[{}] sending heartbeat", currentRole.get());

        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(getCurrentTerm())
                .setLeaderId(getServerId())
                .setLeaderCommitIndex(commitIndex.get());

        LogEntry lastLogEntry = persistence.getLastLogEntry();
        if (lastLogEntry == null) {
            builder.setPrevLogIndex(0L);
            builder.setPrevLogTerm(0L);
        } else {
            builder.setPrevLogIndex(lastLogEntry.getLogIndex());
            builder.setPrevLogTerm(lastLogEntry.getTerm());
        }

        ListenableFuture<List<AppendEntriesResponse>> responses =
                communicator.sendAppendEntriesRequest(builder.build());
        Futures.addCallback(responses, new FutureCallback<List<AppendEntriesResponse>>() {
            @Override
            public void onSuccess(List<AppendEntriesResponse> result) {
                // update terms
                result.stream().filter(Objects::nonNull).forEach((res) -> syncCurrentTerm(res.getTerm()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("[{}] error in getting response from heartbeats: {}", currentRole.get(), t);
            }
        });
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
    private void checkElectionTimeout() throws InterruptedException {
        while (true) {
            final Long hb = heartbeats.poll(electionTimeout, TimeUnit.MILLISECONDS);
            if (hb == null) {
                logger.info("[{}] election timeout, convert to candidate", currentRole.get());
                currentRole.set(Candidate);
                startElection();
                break;
            } else if (hb + electionTimeout > System.currentTimeMillis()) {
                break;
            }
        }
    }


    private void startElection() {
        logger.info("[{}] try to start election, current term {}", currentRole.get(), persistence.getCurrentTerm());
        while (currentRole.get() == Candidate) {
            final long newTerm = persistence.incrementAndGetCurrentTerm();
            logger.info("[{}] increasing to new term {}", currentRole.get(), newTerm);

            final VoteRequest.Builder builder = VoteRequest.newBuilder()
                    .setCandidateId(getServerId())
                    .setCandidateTerm(newTerm);

            final LogEntry lastLogEntry = persistence.getLastLogEntry();
            if (lastLogEntry == null) {
                builder.setLastLogIndex(0L);
                builder.setLastLogTerm(0L);
            } else {
                builder.setLastLogIndex(lastLogEntry.getLogIndex());
                builder.setLastLogTerm(lastLogEntry.getTerm());
            }

            try {
                List<VoteResponse> responses =
                        communicator.sendVoteRequest(builder.build()).get(electionTimeout, TimeUnit.MILLISECONDS);
                // update term if possible
                responses.stream().filter(Objects::nonNull)
                        .forEach((response) -> syncCurrentTerm(response.getTerm()));
                final long numberOfAyes = responses.stream()
                        .filter(Objects::nonNull)
                        .filter(VoteResponse::getVoteGranted)
                        .count();
                logger.info("[{}] number of ayes {} out of {}", getCurrentRole(), numberOfAyes, responses.size());
                if (2 * (numberOfAyes + 1) > responses.size()) {
                    if (currentRole.compareAndSet(Candidate, Leader)) {
                        logger.info("[{}] won election, current term {}", currentRole.get(), persistence.getCurrentTerm());
                        sendHeartbeat();
                        reinitializeLeaderStates();
                        return;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw Throwables.propagate(e);
            } catch (TimeoutException e) {
                logger.info("[{}] election timeout, restart election", currentRole.get());
            }
        }
        logger.info("[{}] lose election, current term {}", currentRole.get(), persistence.getCurrentTerm());
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
    public String getServerId() {
        return communicator.getServerHostAndPort().toString();
    }

    @Override
    protected void runOneIteration() throws Exception {
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

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(electionTimeout, electionTimeout, TimeUnit.MILLISECONDS);
    }
}
