package org.apache.zookeeper.rpc;

import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.rpc.generated.v1.RpcRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.apache.zookeeper.rpc.generated.v1.ZooKeeperServiceGrpc;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RpcZooKeeperService extends ZooKeeperServiceGrpc.ZooKeeperServiceImplBase implements Iterable<ServerCnxn> {
    private final List<RpcServerConnection> connections = new CopyOnWriteArrayList<>();
    private final RpcServerConnectionFactory factory;
    private volatile ZooKeeperServer zooKeeperServer;

    public RpcZooKeeperService(RpcServerConnectionFactory factory) {
        this.factory = factory;
    }

    public void setZooKeeperServer(ZooKeeperServer zooKeeperServer) {
        this.zooKeeperServer = zooKeeperServer;
    }

    @Override
    public StreamObserver<RpcRequest> process(StreamObserver<RpcResponse> responseObserver) {
        InetSocketAddress clientAddress = ClientAddress.KEY.get().get();
        RpcServerConnection connection = new RpcServerConnection(factory, this, zooKeeperServer, responseObserver, clientAddress);
        connections.add(connection);
        return connection;
    }

    @Override
    public Iterator<ServerCnxn> iterator() {
        Iterator<RpcServerConnection> iterator = connections.iterator();
        return new Iterator<ServerCnxn>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public RpcServerConnection next() {
                return iterator.next();
            }
        };
    }

    public void remove(RpcServerConnection connection) {
        connections.remove(connection);
    }
}
