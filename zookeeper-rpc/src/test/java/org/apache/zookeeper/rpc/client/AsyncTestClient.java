package org.apache.zookeeper.rpc.client;

import static org.apache.zookeeper.rpc.client.WatcherService.Type.CHILD;
import static org.apache.zookeeper.rpc.client.WatcherService.Type.DATA;
import static org.apache.zookeeper.rpc.client.WatcherService.Type.EXISTS;
import static org.apache.zookeeper.rpc.client.WatcherService.Type.NONE;
import static org.apache.zookeeper.rpc.client.WatcherService.Type.PERSISTENT;
import static org.apache.zookeeper.rpc.client.WatcherService.Type.PERSISTENT_RECURSIVE;
import static org.apache.zookeeper.rpc.generated.v1.RpcAddWatchMode.ADD_WATCH_MODE_PERSISTENT;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.rpc.generated.v1.RpcACLs;
import org.apache.zookeeper.rpc.generated.v1.RpcAddWatchMode;
import org.apache.zookeeper.rpc.generated.v1.RpcAddWatchRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcCloseSessionRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcConnectRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcConnectResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateMode;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcDeleteRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcExistsRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcFourLetterRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetAllChildrenNumberRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetChildrenRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetConfigRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetDataRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetEphemeralsRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcGetServerMetaDataRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;
import org.apache.zookeeper.rpc.generated.v1.RpcReRegisterWatchersRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcRemoveAllWatchesRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcSetDataRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcSpecialXids;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherType;
import org.apache.zookeeper.rpc.generated.v1.RpcZooDefsIds;
import org.apache.zookeeper.rpc.generated.v1.ZooKeeperServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTestClient implements TestClient {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicLong lastSeenZxid = new AtomicLong(0);
    private final Xid xid = new Xid();
    private final WatcherService watcherService = new WatcherService();
    private final PendingRequestsService pendingRequestsService = new PendingRequestsService(watcherService);
    private final Consumer<RpcResponse> responseHandler;
    private final Supplier<InetSocketAddress> hostProvider;
    private final AtomicReference<ClientState> state = new AtomicReference<>(ClientState.DISCONNECTED);
    private final AtomicReference<PingService> pingService = new AtomicReference<>();
    private final AtomicReference<StreamObserver<RpcRequest>> streamObserver = new AtomicReference<>();
    private volatile ManagedChannel channel;
    private volatile RpcConnectResponse connectionInfo;
    private volatile String chrootPath;

    public AsyncTestClient(Supplier<InetSocketAddress> hostProvider, Consumer<RpcResponse> responseHandler) {
        this.hostProvider = hostProvider;
        this.responseHandler = responseHandler;
    }

    @Override
    public RpcConnectResponse connectionInfo() {
        return connectionInfo;
    }

    @Override
    public ClientState getClientState() {
        return state.get();
    }

    @Override
    public void start() {
        if (!state.compareAndSet(ClientState.DISCONNECTED, ClientState.CONNECTED)) {
            throw new IllegalStateException("Already connected");
        }
        InetSocketAddress address = hostProvider.get();
        channel = ManagedChannelBuilder
                .forAddress(address.getHostString(), address.getPort())
                .usePlaintext()
                .build();
        ZooKeeperServiceGrpc.ZooKeeperServiceStub service = ZooKeeperServiceGrpc.newStub(channel);
        StreamObserver<RpcResponse> newStreamObserver = new StreamObserver<RpcResponse>() {
            @Override
            public void onNext(RpcResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                // TODO
            }

            @Override
            public void onCompleted() {
                stop();
            }
        };
        if (!streamObserver.compareAndSet(null, service.process(newStreamObserver))) {
            throw new IllegalStateException("streamObserver not null");
        }
    }

    @Override
    public boolean awaitTermination(long maxWaitMs) throws InterruptedException {
        if ((channel == null) || channel.awaitTermination(maxWaitMs, TimeUnit.MILLISECONDS)) {
            channel = null;
            return true;
        }
        return false;
    }

    @Override
    public void stop() {
        if (state.compareAndSet(ClientState.CONNECTED, ClientState.DISCONNECTED)) {
            stopPingService();
            pendingRequestsService.clear();
            StreamObserver<RpcRequest> localStreamObserver = streamObserver.getAndSet(null);
            if (localStreamObserver != null) {
                localStreamObserver.onCompleted();
            }
            channel.shutdown();
        }
    }

    @Override
    public void close() {
        if (state.get() == ClientState.DISCONNECTED) {
            throw new IllegalStateException("Not connected");
        }
        postRequest(RpcRequest.newBuilder().setXid(0).setCloseSession(RpcCloseSessionRequest.newBuilder().build()).build(), null, NONE);
    }

    @Override
    public Void connect(int sessionTimeout, String chrootPath) {
        this.chrootPath = (chrootPath != null) ? chrootPath : "";
        RpcConnectRequest connectRequest = RpcConnectRequest.newBuilder()
                .setSessionTimeOut(sessionTimeout)
                .setLastZxidSeen(lastSeenZxid.get())
                .setSessionId(0)
                .setPassword(ByteString.EMPTY)
                .setChrootPath(this.chrootPath)
                .build();
        // important xid for connection is 0
        postRequest(RpcRequest.newBuilder().setXid(0).setConnect(connectRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void reconnect() {
        if (connectionInfo == null) {
            throw new IllegalStateException("Has never been connected");
        }
        RpcConnectRequest connectRequest = RpcConnectRequest.newBuilder()
                .setSessionTimeOut(connectionInfo.getSessionTimeOut())
                .setLastZxidSeen(lastSeenZxid.get())
                .setSessionId(connectionInfo.getSessionId())
                .setPassword(connectionInfo.getPassword())
                .setChrootPath(chrootPath)
                .build();
        postRequest(RpcRequest.newBuilder().setXid(0).setConnect(connectRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void create(String path, RpcCreateMode mode, ByteString data, long ttl) {
        RpcACLs acls = RpcACLs.newBuilder().setPredefinedId(RpcZooDefsIds.IDS_OPEN_ACL_UNSAFE).build();
        RpcCreateRequest createRequest = RpcCreateRequest.newBuilder()
                .setPath(path)
                .setCreateMode(mode)
                .setACLs(acls)
                .setData(data)
                .setTtl(ttl)
                .build();
        postRequest(newRequest().setCreate(createRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void getData(String path, boolean watch) {
        RpcGetDataRequest getDataRequest = RpcGetDataRequest.newBuilder()
                .setPath(path)
                .setWatch(watch)
                .build();
        postRequest(newRequest().setGetData(getDataRequest).build(), path, DATA);
        return null;
    }

    @Override
    public Void setData(String path, ByteString data, int version) {
        RpcSetDataRequest setDataRequest = RpcSetDataRequest.newBuilder()
                .setPath(path)
                .setData(data)
                .setVersion(version)
                .build();
        postRequest(newRequest().setSetData(setDataRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void getChildren(String path, boolean watch) {
        RpcGetChildrenRequest getChildrenRequest = RpcGetChildrenRequest.newBuilder().setPath(path).setWatch(watch).build();
        postRequest(newRequest().setGetChildren(getChildrenRequest).build(), path, CHILD);
        return null;
    }

    @Override
    public Void delete(String path, int version) {
        RpcDeleteRequest deleteRequest = RpcDeleteRequest.newBuilder().setPath(path).setVersion(version).build();
        postRequest(newRequest().setDelete(deleteRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void exists(String path, boolean watch) {
        RpcExistsRequest existsRequest = RpcExistsRequest.newBuilder().setPath(path).setWatch(watch).build();
        postRequest(newRequest().setExists(existsRequest).build(), path, EXISTS);
        return null;
    }

    @Override
    public Void getAllChildrenNumber(String path) {
        RpcGetAllChildrenNumberRequest getAllChildrenNumberRequest = RpcGetAllChildrenNumberRequest.newBuilder().setPath(path).build();
        postRequest(newRequest().setGetAllChildrenNumber(getAllChildrenNumberRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void getEphemerals(String prefixPath) {
        RpcGetEphemeralsRequest getEphemeralsRequest = RpcGetEphemeralsRequest.newBuilder().setPrefixPath(prefixPath).build();
        postRequest(newRequest().setGetEphemerals(getEphemeralsRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void removeAllWatches(String path, RpcWatcherType type, boolean local) {
        RpcRemoveAllWatchesRequest removeAllWatchesRequest = RpcRemoveAllWatchesRequest.newBuilder().setPath(path).setWatcherType(type).setLocal(local).build();
        postRequest(newRequest().setRemoveAllWatches(removeAllWatchesRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void addWatch(String basePath, RpcAddWatchMode mode) {
        RpcAddWatchRequest addWatchRequest = RpcAddWatchRequest.newBuilder().setBasePath(basePath).setMode(mode).build();
        postRequest(newRequest().setAddWatch(addWatchRequest).build(), basePath, (mode == ADD_WATCH_MODE_PERSISTENT) ? PERSISTENT : PERSISTENT_RECURSIVE);
        return null;
    }

    @Override
    public Void transaction(RpcTransactionRequest transaction) {
        postRequest(newRequest().setTransaction(transaction).build(), null, NONE);
        return null;
    }

    @Override
    public Void fourLetter(String word) {
        RpcFourLetterRequest fourLetterRequest = RpcFourLetterRequest.newBuilder().setWord(word).build();
        postRequest(newRequest().setFourLetter(fourLetterRequest).build(), null, NONE);
        return null;
    }

    @Override
    public Void getConfig(boolean watch) {
        RpcGetConfigRequest getConfigRequest = RpcGetConfigRequest.newBuilder().setWatch(watch).build();
        postRequest(newRequest().setGetConfig(getConfigRequest).build(), ZooDefs.CONFIG_NODE, CHILD);
        return null;
    }

    @Override
    public Void getServerMetaData() {
        RpcGetServerMetaDataRequest getServerMetaDataRequest = RpcGetServerMetaDataRequest.newBuilder().build();
        postRequest(newRequest().setGetServerMetaData(getServerMetaDataRequest).build(), null, NONE);
        return null;
    }

    @Override
    public void ping() {
        postRequest(RpcRequest.newBuilder().setXid(RpcSpecialXids.SPECIAL_XID_PING_XID_VALUE).build(), null, NONE);
    }

    @Override
    public void reRegisterWatchers(Set<String> dataWatches,
                                   Set<String> existWatches,
                                   Set<String> childWatches,
                                   Set<String> persistentWatches,
                                   Set<String> persistentRecursiveWatches) {
        RpcReRegisterWatchersRequest reRegisterWatchersRequest = RpcReRegisterWatchersRequest.newBuilder()
                .setLastZxid(lastSeenZxid.get())
                .addAllDataWatches(dataWatches)
                .addAllExistWatches(existWatches)
                .addAllChildWatches(childWatches)
                .addAllPersistentWatches(persistentWatches)
                .addAllPersistentRecursiveWatches(persistentRecursiveWatches)
                .build();
        postRequest(newRequest().setReRegisterWatchers(reRegisterWatchersRequest).build(), null, NONE);
    }

    @Override
    public void startPingService() {
        RpcConnectResponse localConnectionInfo = connectionInfo;
        if (localConnectionInfo == null) {
            throw new IllegalStateException("Not connected");
        }

        PingService newPingService = newPingService(localConnectionInfo.getSessionTimeOut());
        if (pingService.compareAndSet(null, newPingService)) {
            newPingService.start();
        }
    }

    @VisibleForTesting
    protected PingService newPingService(int sessionTimeOut) {
        return new PingServiceImpl(this, sessionTimeOut);
    }

    @Override
    public void stopPingService() {
        PingService localPingService = pingService.getAndSet(null);
        if (localPingService != null) {
            localPingService.close();
        }
    }

    private RpcRequest.Builder newRequest() {
        return RpcRequest.newBuilder().setXid(xid.getXid());
    }

    private void postRequest(RpcRequest request, String path, WatcherService.Type type) {
        StreamObserver<RpcRequest> localStreamObserver = streamObserver.get();
        if (localStreamObserver == null) {
            throw new ZooKeeperException(RpcKeeperExceptionCode.CODE_CONNECTIONLOSS);
        }
        PingService localPingService = pingService.get();
        if (localPingService != null) {
            localPingService.noteRequestSent();
        }
        pendingRequestsService.noteOutgoingRequest(request, path, type);
        localStreamObserver.onNext(request);
    }

    private void handleResponse(RpcResponse response) {
        PingService localPingService = pingService.get();
        if (localPingService != null) {
            localPingService.noteResponseReceived();
        }
        pendingRequestsService.noteIncomingResponse(response);

        if (response.getMessageCase() == RpcResponse.MessageCase.CONNECT) {
            connectionInfo = response.getConnect();
            startPingService();
        }
        if ((response.getXid() != RpcSpecialXids.SPECIAL_XID_PING_XID_VALUE)) {
            responseHandler.accept(response);
        }
    }
}
