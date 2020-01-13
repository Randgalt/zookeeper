package org.apache.zookeeper.rpc.client;

import static java.util.function.Function.identity;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.zookeeper.rpc.generated.v1.RpcAddWatchMode;
import org.apache.zookeeper.rpc.generated.v1.RpcConnectResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateMode;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcExistsResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcFourLetterResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetAllChildrenNumberResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetChildrenResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetConfigResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetDataResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetEphemeralsResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcGetServerMetaDataResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcSetDataResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherEventResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherType;

public class SyncTestClient implements TestClient {
    private final AsyncTestClient testClient;
    private final Duration responseTimeout;
    private final Consumer<RpcWatcherEventResponse> watcherHandler;
    private final BlockingQueue<RpcResponse> responses = new LinkedBlockingQueue<>();

    public static final long DEFAULT_RESPONSE_TIMEOUT_MS = 5000;

    public SyncTestClient(Supplier<InetSocketAddress> hostProvider, Consumer<RpcWatcherEventResponse> watcherHandler) {
        this(proc -> new AsyncTestClient(hostProvider, proc), Duration.ofMillis(DEFAULT_RESPONSE_TIMEOUT_MS), watcherHandler);
    }

    public SyncTestClient(Supplier<InetSocketAddress> hostProvider, Duration responseTimeout, Consumer<RpcWatcherEventResponse> watcherHandler) {
        this(proc -> new AsyncTestClient(hostProvider, proc), responseTimeout, watcherHandler);
    }

    @VisibleForTesting
    public SyncTestClient(Function<Consumer<RpcResponse>, AsyncTestClient> clientProc, Duration responseTimeout, Consumer<RpcWatcherEventResponse> watcherHandler) {
        testClient = clientProc.apply(this::acceptResponse);
        this.responseTimeout = responseTimeout;
        this.watcherHandler = watcherHandler;
    }

    @Override
    public RpcConnectResponse connectionInfo() {
        return testClient.connectionInfo();
    }

    @Override
    public ClientState getClientState() {
        return testClient.getClientState();
    }

    @Override
    public void start() {
        testClient.start();
    }

    @Override
    public boolean awaitTermination(long maxWaitMs) throws InterruptedException {
        return testClient.awaitTermination(maxWaitMs);
    }

    @Override
    public void stop() {
        testClient.stop();
        responses.clear();
    }

    @Override
    public void close() {
        testClient.close();
        try {
            RpcResponse response = nextResponse(identity(), false);
        } catch (RuntimeException e) {
            // TODO - log
        }
    }

    @Override
    public RpcConnectResponse connect(int sessionTimeout, String chrootPath) {
        testClient.connect(sessionTimeout, chrootPath);
        RpcResponse response = nextResponse(identity());
        if (!response.hasConnect()) {
            throw new IllegalStateException("Unexpected connect response: " + response);
        }
        if (response.getError() != RpcKeeperExceptionCode.CODE_OK) {
            throw new IllegalStateException("Connection failed: " + response.getError());
        }
        return response.getConnect();
    }

    @Override
    public RpcConnectResponse reconnect() {
        testClient.reconnect();
        return nextResponse(RpcResponse::getConnect);
    }

    @Override
    public RpcCreateResponse create(String path, RpcCreateMode mode, ByteString data, long ttl) {
        testClient.create(path, mode, data, ttl);
        return nextResponse(RpcResponse::getCreate);
    }

    @Override
    public RpcGetDataResponse getData(String path, boolean watch) {
        testClient.getData(path, watch);
        return nextResponse(RpcResponse::getGetData);
    }

    @Override
    public RpcSetDataResponse setData(String path, ByteString data, int version) {
        testClient.setData(path, data, version);
        return nextResponse(RpcResponse::getSetData);
    }

    @Override
    public RpcGetChildrenResponse getChildren(String path, boolean watch) {
        testClient.getChildren(path, watch);
        return nextResponse(RpcResponse::getGetChildren);
    }

    @Override
    public RpcResponse delete(String path, int version) {
        testClient.delete(path, version);
        return nextResponse(identity());
    }

    @Override
    public RpcExistsResponse exists(String path, boolean watch) {
        testClient.exists(path, watch);
        RpcResponse response = nextResponse(identity(), false);
        if (response.getError() == RpcKeeperExceptionCode.CODE_NONODE) {
            return RpcExistsResponse.newBuilder().build();
        }
        if (response.getError() != RpcKeeperExceptionCode.CODE_OK) {
            throw new ZooKeeperException(response.getError());
        }
        return response.getExists();
    }

    @Override
    public RpcGetAllChildrenNumberResponse getAllChildrenNumber(String path) {
        testClient.getAllChildrenNumber(path);
        return nextResponse(RpcResponse::getGetAllChildrenNumber);
    }

    @Override
    public RpcGetEphemeralsResponse getEphemerals(String prefixPath) {
        testClient.getEphemerals(prefixPath);
        return nextResponse(RpcResponse::getGetEphemerals);
    }

    @Override
    public RpcResponse removeAllWatches(String path, RpcWatcherType type, boolean local) {
        testClient.removeAllWatches(path, type, local);
        return nextResponse(identity());
    }

    @Override
    public RpcResponse addWatch(String basePath, RpcAddWatchMode mode) {
        testClient.addWatch(basePath, mode);
        return nextResponse(identity());
    }

    @Override
    public RpcTransactionResponse transaction(RpcTransactionRequest transaction) {
        testClient.transaction(transaction);
        return nextResponse(RpcResponse::getTransaction);
    }

    @Override
    public RpcFourLetterResponse fourLetter(String word) {
        testClient.fourLetter(word);
        return nextResponse(RpcResponse::getFourLetter);
    }

    @Override
    public RpcGetConfigResponse getConfig(boolean watch) {
        testClient.getConfig(watch);
        return nextResponse(RpcResponse::getGetConfig);
    }

    @Override
    public RpcGetServerMetaDataResponse getServerMetaData() {
        testClient.getServerMetaData();
        return nextResponse(RpcResponse::getGetServerMetaData);
    }

    @Override
    public void ping() {
        testClient.ping();
    }

    @Override
    public void reRegisterWatchers(Set<String> dataWatches,
                                   Set<String> existWatches,
                                   Set<String> childWatches,
                                   Set<String> persistentWatches,
                                   Set<String> persistentRecursiveWatches) {
        testClient.reRegisterWatchers(dataWatches, existWatches, childWatches, persistentWatches, persistentRecursiveWatches);
        nextResponse(identity());
    }

    @Override
    public void startPingService() {
        testClient.startPingService();
    }

    @Override
    public void stopPingService() {
        testClient.stopPingService();
    }

    private void acceptResponse(RpcResponse response) {
        if (response.getMessageCase() == RpcResponse.MessageCase.WATCHEREVENT) {
            watcherHandler.accept(response.getWatcherEvent());
        } else {
            responses.add(response);
        }
    }

    private <T> T nextResponse(Function<RpcResponse, T> proc) {
        return nextResponse(proc, true);
    }

    private <T> T nextResponse(Function<RpcResponse, T> proc, boolean throwOnError) {
        try {
            RpcResponse response = responses.poll(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new RuntimeException("Timed out waiting for response");
            }

            if (throwOnError && (response.getError() != RpcKeeperExceptionCode.CODE_OK)) {
                throw new ZooKeeperException(response.getError());
            }

            T appliedResponse = proc.apply(response);
            if (appliedResponse == null) {
                throw new RuntimeException("Getter proc returned a null response");
            }
            return appliedResponse;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted");  // TODO
        }
    }

}
