package org.apache.zookeeper.rpc.client;

import com.google.protobuf.ByteString;
import org.apache.zookeeper.rpc.generated.v1.RpcAddWatchMode;
import org.apache.zookeeper.rpc.generated.v1.RpcConnectResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateMode;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherType;

import java.io.Closeable;
import java.util.Set;

public interface TestClient extends Closeable {
    RpcConnectResponse connectionInfo();

    ClientState getClientState();

    void start();

    void stop();

    boolean awaitTermination(long maxWaitMs) throws InterruptedException;

    @Override
    void close();

    Object connect(int sessionTimeout, String chrootPath);

    Object reconnect();

    Object create(String path, RpcCreateMode mode, ByteString data, long ttl);

    Object getData(String path, boolean watch);

    Object setData(String path, ByteString data, int version);

    Object getChildren(String path, boolean watch);

    Object delete(String path, int version);

    Object exists(String path, boolean watch);

    Object getAllChildrenNumber(String path);

    Object getEphemerals(String prefixPath);

    Object removeAllWatches(String path, RpcWatcherType type, boolean local);

    Object addWatch(String basePath, RpcAddWatchMode mode);

    Object transaction(RpcTransactionRequest transaction);

    Object fourLetter(String word);

    Object getConfig(boolean watch);

    Object getServerMetaData();

    void ping();

    void reRegisterWatchers(Set<String> dataWatches,
                            Set<String> existWatches,
                            Set<String> childWatches,
                            Set<String> persistentWatches,
                            Set<String> persistentRecursiveWatches);

    void startPingService();

    void stopPingService();
}
