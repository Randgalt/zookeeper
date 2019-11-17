package org.apache.zookeeper.grpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class RpcServerCnxnFactory extends ServerCnxnFactory implements ServerInterceptor {
    private volatile InetSocketAddress localAddress;
    private volatile int maxClientCnxns;
    private volatile int backlog;
    private volatile boolean secure;
    private volatile Server server;

    @Override
    public int getLocalPort() {
        return server.getPort();
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return null;
    }

    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) {
        this.localAddress = addr;
        this.maxClientCnxns = maxcc;
        this.backlog = backlog;
        this.secure = secure;
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {

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
        server = ServerBuilder.forPort(localAddress.getPort())
                .addService(new RpcZooKeeperServer(zkServer))
                .intercept(this)
                .build();
        if (zkServer != null) {
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
        try {
            server.start();
        } catch (IOException e) {
            // TODO
        }
    }

    @Override
    public void closeAll(ServerCnxn.DisconnectReason reason) {
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public void resetAllConnectionStats() {

    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        return null;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        SocketAddress socketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        Context context = Context.current().withValue(CallContext.KEY, new CallContext((InetSocketAddress) socketAddress));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
