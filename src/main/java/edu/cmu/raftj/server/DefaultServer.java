package edu.cmu.raftj.server;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.*;
import edu.cmu.raftj.rpc.RequestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Maps.asMap;
import static edu.cmu.raftj.server.Server.Role.*;

/**
 * Default implementation for {@link Server}
 */
public class DefaultServer extends AbstractScheduledService implements Server, RequestListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultServer.class);
    private final BlockingDeque<Long> heartbeats = Queues.newLinkedBlockingDeque();
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Follower);
    private final AtomicReference<String> currentLeaderID = new AtomicReference<>(null);
    private final Communicator communicator;
    private final Persistence persistence;
    @GuardedBy("synchronization block")
    private final Map<HostAndPort, Long> nextIndices = Maps.newHashMap();
    @GuardedBy("synchronization block")
    private final Map<HostAndPort, Long> matchIndices = Maps.newHashMap();
    private final Random random = new Random();
    private final StateMachine stateMachine;

    public DefaultServer(StateMachine stateMachine, Communicator communicator, Persistence persistence) {
        this.stateMachine = checkNotNull(stateMachine, "state machine");
        this.communicator = checkNotNull(communicator, "communicator");
        this.persistence = checkNotNull(persistence, "persistence");
    }

    /**
     * sync current term with a potential larger term
     *
     * @param term term
     */
    private void syncCurrentTerm(long term, String senderID) {
        if (persistence.largerThanAndSetCurrentTerm(term)) {
            final Role oldRole = currentRole.getAndSet(Follower);
            currentLeaderID.set(senderID);
            logger.info("[{}] bumped current term to be {}, role was {}",
                    getCurrentRole(), term, oldRole);
        }
    }

    @Override
    public VoteResponse onVoteRequest(VoteRequest voteRequest) {
        logger.info("[{}] got vote request {}", getCurrentRole(), voteRequest);
        syncCurrentTerm(voteRequest.getCandidateTerm(), voteRequest.getCandidateId());

        final VoteResponse.Builder builder = VoteResponse.newBuilder().setVoteGranted(false);
        final String candidateId = checkNotNull(voteRequest.getCandidateId(), "candidate ID");
        final boolean upToDate = voteRequest.getLastLogIndex() >= persistence.getLastLogIndex();
        if (voteRequest.getCandidateTerm() >= getCurrentTerm() && upToDate) {
            if (Objects.equals(candidateId, persistence.getVotedForInCurrentTerm()) ||
                    persistence.compareAndSetVoteFor(null, candidateId)) {
                logger.info("[{}] voted yes for {}", getCurrentRole(), candidateId);
                builder.setVoteGranted(true);
            } else {
                logger.info("[{}] voted no for {} because already voted for {}",
                        getCurrentRole(), candidateId, persistence.getVotedForInCurrentTerm());
            }
        } else if (upToDate) {
            logger.info("[{}] voted no for {} because the term was too small, {} < {}",
                    getCurrentRole(), candidateId, voteRequest.getCandidateTerm(), getCurrentTerm());
        } else {
            logger.info("[{}] voted no for {} because the log is not up to date",
                    getCurrentRole(), candidateId);
        }
        return builder.setSenderID(getServerId()).setTerm(getCurrentTerm()).build();
    }

    private void updateCommitIndex(AppendEntriesRequest request) {
        long leaderCommitIndex = request.getLeaderCommitIndex();
        final List<LogEntry> entryList = request.getLogEntriesList();
        if (!entryList.isEmpty()) {
            leaderCommitIndex = Math.min(leaderCommitIndex, getLast(entryList).getLogIndex());
        }
        stateMachine.increaseCommitIndex(leaderCommitIndex);
    }

    @Override
    public AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest request) {
        logger.info("[{}] append entries request / heartbeat from {}, term {}",
                getCurrentRole(), request.getLeaderId(), request.getLeaderTerm());
        syncCurrentTerm(request.getLeaderTerm(), request.getLeaderId());
        heartbeats.addLast(System.currentTimeMillis());

        final AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder().setSuccess(false);
        // the term is new
        if (request.getLeaderTerm() >= getCurrentTerm()) {
            final LogEntry lastEntry = persistence.getLastLogEntry();
            if (lastEntry == null) {
                request.getLogEntriesList().stream().forEach(persistence::applyLogEntry);
                updateCommitIndex(request);
                builder.setSuccess(true);
            } else if (lastEntry.getLogIndex() >= request.getPrevLogIndex()) {
                final LogEntry entry = persistence.getLogEntry(request.getPrevLogIndex());
                if (entry.getTerm() == request.getPrevLogTerm()) {
                    request.getLogEntriesList().stream().forEach(persistence::applyLogEntry);
                    updateCommitIndex(request);
                    builder.setSuccess(true);
                } else {
                    logger.warn("[{}] prev entry mismatch, local last term {}, remote prev term {}",
                            getCurrentRole(), entry.getTerm(), request.getPrevLogTerm());
                }
            } else {
                logger.warn("[{}] log entries are too new, remote prev index {}",
                        getCurrentRole(),
                        request.getPrevLogIndex());
            }
        }
        return builder.setSenderID(getServerId()).setTerm(getCurrentTerm()).build();
    }

    @Override
    public ClientMessageResponse onClientCommand(String command) {
        logger.info("[{}] client command {}", getCurrentRole(), checkNotNull(command, "command"));
        if (getCurrentRole() == Leader) {
            LogEntry logEntry = LogEntry.newBuilder()
                    .setCommand(command)
                    .setTerm(getCurrentTerm())
                    .setLogIndex(persistence.getLastLogIndex() + 1)
                    .build();
            persistence.applyLogEntry(logEntry);
            sendAndWaitLogReplications();
            updateCommitIndexAfterReplications();
            return ClientMessageResponse.newBuilder()
                    .setSuccess(true)
                    .build();
        } else {
            return ClientMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderID(currentLeaderID.get())
                    .build();
        }
    }

    private AppendEntriesRequest buildReplicationRequest(long from) {
        ImmutableList<LogEntry> entries = persistence.getLogEntriesFrom(from);
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(getCurrentTerm())
                .setLeaderId(getServerId())
                .setLeaderCommitIndex(stateMachine.getCommitIndex())
                .addAllLogEntries(entries);
        if (from > 1) {
            final LogEntry prev = persistence.getLogEntry(from - 1);
            builder.setPrevLogIndex(prev.getLogIndex()).setPrevLogTerm(prev.getTerm());
        } else {
            builder.setPrevLogTerm(0L).setPrevLogIndex(0L);
        }
        return builder.build();
    }

    private void sendAndWaitLogReplications() {
        final Deque<HostAndPort> nextFollower = Queues.newArrayDeque(communicator.getAudience());
        while (!nextFollower.isEmpty()) {
            final HostAndPort nextAudience = nextFollower.removeFirst();
            final long nextIndex;
            synchronized (nextIndices) {
                nextIndex = nextIndices.get(nextAudience);
            }
            final long lastIndex = persistence.getLastLogIndex();
            if (nextIndex <= lastIndex) {
                try {
                    AppendEntriesResponse response =
                            communicator.sendAppendEntriesRequest(buildReplicationRequest(nextIndex), nextAudience).get();
                    if (!response.getSuccess()) {
                        logger.warn("[{}] follower {} responded false for append entries request, " +
                                        "decrement next index to {} and retry",
                                getCurrentRole(), nextIndex - 1);
                        synchronized (nextIndices) {
                            nextIndices.put(nextAudience, nextIndex - 1);
                        }
                        nextFollower.addLast(nextAudience);
                    } else {
                        logger.info("[{}] successfully replicated logs [{}, {}) to follower {}",
                                getCurrentRole(), nextIndex, lastIndex + 1, nextAudience);
                        synchronized (nextIndices) {
                            nextIndices.put(nextAudience, lastIndex + 1);
                        }
                        synchronized (matchIndices) {
                            matchIndices.put(nextAudience, lastIndex + 1);
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("[{}] failed to send log replications to {}, retrying", getCurrentRole(), nextAudience);
                    nextFollower.addLast(nextAudience);
                }
            }
        }
    }

    private void updateCommitIndexAfterReplications() {
        final long min, max;
        synchronized (matchIndices) {
            min = Math.max(stateMachine.getCommitIndex(),
                    matchIndices.values().stream().min(Comparator.<Long>naturalOrder()).get());
            max = matchIndices.values().stream().max(Comparator.<Long>naturalOrder()).get();
        }
        for (long idx = max; idx >= min; --idx) {
            final boolean isMajority;
            synchronized (matchIndices) {
                final long finalIdx = idx;
                final long count = matchIndices.values().stream().filter((v) -> v >= finalIdx).count();
                isMajority = 2 * (count + 1) > (matchIndices.size() + 1);
            }
            if (isMajority && persistence.getLogEntry(idx).getTerm() == getCurrentTerm()) {
                stateMachine.increaseCommitIndex(idx);
                logger.info("[{}] increase commit index to {}", getCurrentRole(), idx);
                break;
            }
        }
    }

    /**
     * leader sends heartbeat
     */
    private void sendHeartbeat() {
        logger.info("[{}] sending heartbeat", getCurrentRole());
        AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder()
                .setLeaderTerm(getCurrentTerm())
                .setLeaderId(getServerId())
                .setLeaderCommitIndex(stateMachine.getCommitIndex());
        LogEntry lastLogEntry = persistence.getLastLogEntry();
        if (lastLogEntry == null) {
            builder.setPrevLogIndex(0L);
            builder.setPrevLogTerm(0L);
        } else {
            builder.setPrevLogIndex(lastLogEntry.getLogIndex());
            builder.setPrevLogTerm(lastLogEntry.getTerm());
        }
        final AppendEntriesRequest request = builder.build();
        communicator.getAudience().stream().forEach((follower) -> {
            ListenableFuture<AppendEntriesResponse> response = communicator.sendAppendEntriesRequest(request, follower);
            Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {
                @Override
                public void onSuccess(AppendEntriesResponse result) {
                    syncCurrentTerm(result.getTerm(), result.getSenderID());
                    // logger.info("[{}] successfully got result of append entries {}", getCurrentRole(), result);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("[{}] error in getting response from heartbeats: {}", getCurrentRole(), t);
                }
            });
        });
    }

    private void reinitializeLeaderStates() {
        synchronized (nextIndices) {
            nextIndices.putAll(asMap(communicator.getAudience(), (x) -> persistence.getLastLogIndex() + 1));
        }
        synchronized (matchIndices) {
            matchIndices.putAll(asMap(communicator.getAudience(), (x) -> 0L));
        }
    }

    /**
     * followers check election timeout, block for up to election timeout millis if not available
     */
    private void checkElectionTimeout() throws InterruptedException {
        while (true) {
            final long electionTimeout = getElectionTimeout();
            final Long hb = heartbeats.poll(electionTimeout, TimeUnit.MILLISECONDS);
            if (hb == null) {
                if (currentRole.compareAndSet(Follower, Candidate)) {
                    logger.info("[{}] election timeout, converted to candidate", getCurrentRole());
                    startElection();
                }
                break;
            } else if (hb + electionTimeout > System.currentTimeMillis()) {
                break;
            }
        }
    }

    /**
     * start election while the role is candidate, abort earlier if that's no longer the case
     */
    private void startElection() {
        logger.info("[{}] try to start election, current term {}",
                getCurrentRole(), getCurrentTerm());
        while (getCurrentRole() == Candidate) {
            final long newTerm = persistence.incrementAndGetCurrentTerm();
            logger.info("[{}] start election, increasing to new term {}", getCurrentRole(), newTerm);

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
                List<VoteResponse> responses = communicator.sendVoteRequest(builder.build())
                        .get(getElectionTimeout(), TimeUnit.MILLISECONDS);
                // update term if possible
                responses.stream().filter(Objects::nonNull)
                        .forEach((response) -> syncCurrentTerm(response.getTerm(), response.getSenderID()));
                final long numberOfAyes = responses.stream()
                        .filter(Objects::nonNull)
                        .filter(VoteResponse::getVoteGranted)
                        .count();
                final long numberOfInvalids = responses.stream().filter(Objects::isNull).count();
                logger.info("[{}] number of ayes {} out of {}, {} failed to respond",
                        getCurrentRole(),
                        numberOfAyes + 1,
                        responses.size() + 1,
                        numberOfInvalids);
                if (2 * (numberOfAyes + 1) > (responses.size() + 1)) {
                    if (currentRole.compareAndSet(Candidate, Leader)) {
                        logger.info("[{}] won election, current term {}",
                                getCurrentRole(), getCurrentTerm());
                        reinitializeLeaderStates();
                        sendHeartbeat();
                        // done, return
                        return;
                    } else {
                        logger.info("[{}] election aborted", getCurrentRole());
                    }
                } else {
                    logger.info("[{}] failed to get a majority, retrying...", getCurrentRole());
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ConnectException) {
                    logger.info("[{}] connect exception, retry", getCurrentRole());
                } else {
                    throw Throwables.propagate(e);
                }
            } catch (TimeoutException e) {
                logger.info("[{}] timeout waiting for vote responses, retry vote request", getCurrentRole());
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
        logger.info("[{}] lost election, current term {}", getCurrentRole(), getCurrentTerm());
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
        stateMachine.applyAllPendingCommandsFrom(persistence);
        final Role role = getCurrentRole();
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
    protected String serviceName() {
        return getServerId();
    }

    private long getElectionTimeout() {
        return 150 + random.nextInt(150);
    }

    @Override
    protected Scheduler scheduler() {
        final long heartbeatTimeout = 50L;
        return Scheduler.newFixedDelaySchedule(0, heartbeatTimeout, TimeUnit.MILLISECONDS);
    }
}
