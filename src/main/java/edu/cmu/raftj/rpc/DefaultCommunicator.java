package edu.cmu.raftj.rpc;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.*;
import com.google.protobuf.GeneratedMessage;
import edu.cmu.raftj.rpc.Messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Default {@link Communicator} implementation that runs server on a dedicated thread, and boardCasts
 * requests concurrently. This implementation does not reuse socket and connections.
 */
public class DefaultCommunicator extends AbstractExecutionThreadService implements Communicator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommunicator.class);

    private final HostAndPort hostAndPort;
    private final ImmutableSet<HostAndPort> audience;
    private final ListeningExecutorService broadcastExecutor;
    private final ServerSocket serverSocket;
    private RequestListener requestListener;

    public DefaultCommunicator(HostAndPort hostAndPort, Set<HostAndPort> audience) throws IOException {
        this(new ServerSocket(hostAndPort.getPort(), 50, InetAddress.getByName(hostAndPort.getHostText())), audience);
    }

    DefaultCommunicator(ServerSocket serverSocket, Set<HostAndPort> audience) throws IOException {
        this.serverSocket = checkNotNull(serverSocket);
        this.serverSocket.setReuseAddress(true);
        this.audience = ImmutableSet.copyOf(audience);
        hostAndPort = HostAndPort.fromParts(serverSocket.getInetAddress().getHostName(),
                serverSocket.getLocalPort());
        checkArgument(!audience.contains(hostAndPort),
                "server audiences %s cannot contain this server %s",
                audience, hostAndPort);
        broadcastExecutor = listeningDecorator(newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(hostAndPort + " BroadcastThread")
                        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error("exception thrown", e);
                                System.exit(-1);
                            }
                        }).build()
        ));
    }


    private <T extends GeneratedMessage> ListenableFuture<List<T>> boardCast(Request request, Function<InputStream, T> builder) {
        final List<ListenableFuture<T>> list = Lists.newArrayList();
        for (HostAndPort hostAndPort : audience) {
            final SettableFuture<T> settableFuture = SettableFuture.create();
            broadcastExecutor.execute(() -> {
                try (final Socket socket = new Socket(InetAddress.getByName(hostAndPort.getHostText()), hostAndPort.getPort());
                     final OutputStream outputStream = socket.getOutputStream();
                     final InputStream inputStream = socket.getInputStream()) {
                    request.writeDelimitedTo(outputStream);
                    settableFuture.set(builder.apply(inputStream));
                } catch (Exception e) {
                    logger.warn("error in sending vote request to {}, exception {}",
                            hostAndPort, Throwables.getStackTraceAsString(e));
                    settableFuture.setException(e);
                }
            });
            list.add(settableFuture);
        }
        return Futures.successfulAsList(list);
    }

    @Override
    public ListenableFuture<List<VoteResponse>> sendVoteRequest(VoteRequest voteRequest) {
        return boardCast(Request.newBuilder().setVoteRequest(voteRequest).build(), (is) -> {
            try {
                return VoteResponse.parseDelimitedFrom(is);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public ListenableFuture<List<AppendEntriesResponse>> sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        return boardCast(Request.newBuilder().setAppendEntriesRequest(appendEntriesRequest).build(), (is) -> {
            try {
                return AppendEntriesResponse.parseDelimitedFrom(is);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public void setRequestListener(RequestListener requestListener) {
        checkState(!isRunning(), "must be set before run");
        this.requestListener = checkNotNull(requestListener, "request listener");
    }

    @Override
    public HostAndPort getServerHostAndPort() {
        return hostAndPort;
    }

    @Override
    public ImmutableSet<HostAndPort> getAudience() {
        return audience;
    }

    @Override
    protected void startUp() throws Exception {
        serverSocket.setReuseAddress(true);
    }

    @Override
    protected void run() throws Exception {
        checkState(null != requestListener, "request listener not set");
        checkState(serverSocket.isBound(), "server socket must be bound");
        while (isRunning()) {
            try (final Socket client = serverSocket.accept();
                 final InputStream inputStream = client.getInputStream();
                 final OutputStream outputStream = client.getOutputStream()) {
                final Request request = Request.parseDelimitedFrom(inputStream);
                logger.info("handling {} request from {}:{}",
                        request.getPayloadCase(), client.getInetAddress(), client.getPort());
                switch (request.getPayloadCase()) {
                    case APPENDENTRIESREQUEST: {
                        requestListener.onAppendEntriesRequest(request.getAppendEntriesRequest()).writeDelimitedTo(outputStream);
                        break;
                    }
                    case VOTEREQUEST: {
                        requestListener.onVoteRequest(request.getVoteRequest()).writeDelimitedTo(outputStream);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("payload not set");
                }
            } catch (Exception e) {
                logger.warn("exception in accepting client", e);
            }
        }
    }

    @Override
    protected String serviceName() {
        return hostAndPort.toString() + " Communicator";
    }

    @Override
    protected void shutDown() throws Exception {
        serverSocket.close();
    }
}
