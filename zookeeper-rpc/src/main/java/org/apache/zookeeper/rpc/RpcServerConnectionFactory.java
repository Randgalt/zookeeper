package org.apache.zookeeper.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class RpcServerConnectionFactory extends ServerCnxnFactory implements ServerInterceptor {
    private volatile InetSocketAddress localAddress;
    private volatile int maxClientCnxns;
    private volatile int backlog;
    private volatile boolean secure;
    private volatile Server server;
    private volatile RpcZooKeeperService rpcZooKeeperService;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public int getLocalPort() {
        return localAddress.getPort();
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return rpcZooKeeperService;
    }

    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) {
        this.localAddress = addr;
        this.maxClientCnxns = maxcc;
        this.backlog = backlog;
        this.secure = secure;

        rpcZooKeeperService = new RpcZooKeeperService(this);
        server = NettyServerBuilder.forPort(localAddress.getPort())
                .addService(rpcZooKeeperService)
                .intercept(this)
                .build();
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        // TODO
    }

    @Override
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    @Override
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    @Override
    public void startup(ZooKeeperServer zkServer, boolean startServer) throws IOException, InterruptedException {
        if (zkServer != null) {
            rpcZooKeeperService.setZooKeeperServer(zkServer);
            if (secure) {
                zkServer.setSecureServerCnxnFactory(this);
            } else {
                zkServer.setServerCnxnFactory(this);
            }
            if (startServer) {
                start();
                zkServer.startdata();
                zkServer.startup();
            }
        }
    }

    @Override
    public int getSocketListenBacklog() {
        return backlog;
    }

    @Override
    public void join() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void shutdown() {
        server.shutdown();
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            try {
                server.start();
            } catch (IOException e) {
                // TODO
            }
        }
    }

    @Override
    public void closeAll(ServerCnxn.DisconnectReason reason) {
        rpcZooKeeperService.forEach(s -> s.close(reason));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public void resetAllConnectionStats() {
        // TODO
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        // TODO
        return Collections.emptySet();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        // the only way to get the caller address is via this intercept mechanism
        SocketAddress socketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

        // add the caller address to the current context so that RpcZooKeeperService has access to it
        Context context = Context.current().withValue(ClientAddress.KEY, new ClientAddress((InetSocketAddress) socketAddress));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
