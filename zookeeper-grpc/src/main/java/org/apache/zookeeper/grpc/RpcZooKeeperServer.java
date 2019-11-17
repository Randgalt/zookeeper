package org.apache.zookeeper.grpc;

import static org.apache.zookeeper.ZooDefs.OpCode.addWatch;
import static org.apache.zookeeper.ZooDefs.OpCode.auth;
import static org.apache.zookeeper.ZooDefs.OpCode.createTTL;
import static org.apache.zookeeper.ZooDefs.OpCode.delete;
import static org.apache.zookeeper.ZooDefs.OpCode.exists;
import static org.apache.zookeeper.ZooDefs.OpCode.getACL;
import static org.apache.zookeeper.ZooDefs.OpCode.getAllChildrenNumber;
import static org.apache.zookeeper.ZooDefs.OpCode.getChildren2;
import static org.apache.zookeeper.ZooDefs.OpCode.getData;
import static org.apache.zookeeper.ZooDefs.OpCode.getEphemerals;
import static org.apache.zookeeper.ZooDefs.OpCode.setACL;
import static org.apache.zookeeper.ZooDefs.OpCode.setData;
import static org.apache.zookeeper.ZooDefs.OpCode.sync;
import static org.apache.zookeeper.grpc.RpcMappers.toAddWatchMode;
import static org.apache.zookeeper.grpc.RpcMappers.toAddWatchRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toAuthInfo;
import static org.apache.zookeeper.grpc.RpcMappers.toConnectRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toCreateRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toCreateTTLRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toDeleteRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toExistsRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toGetAclRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toGetAllChildrenNumberRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toGetChildrenRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toGetDataRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toGetEphemeralsRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toSetAclRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toSetDataRequest;
import static org.apache.zookeeper.grpc.RpcMappers.toSyncRequest;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.grpc.stub.StreamObserver;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.grpc.generated.RpcConnectRequest;
import org.apache.zookeeper.grpc.generated.RpcCreateMode;
import org.apache.zookeeper.grpc.generated.RpcCreateRequest;
import org.apache.zookeeper.grpc.generated.RpcRequest;
import org.apache.zookeeper.grpc.generated.RpcRequestHeader;
import org.apache.zookeeper.grpc.generated.RpcResponse;
import org.apache.zookeeper.grpc.generated.ZooKeeperServiceGrpc;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.server.ZooKeeperServer;

public class RpcZooKeeperServer extends ZooKeeperServiceGrpc.ZooKeeperServiceImplBase {
    private final ZooKeeperServer zooKeeperServer;
    private final Map<InetSocketAddress, RpcServerCnxn> connections = new ConcurrentHashMap<>();

    public RpcZooKeeperServer(ZooKeeperServer zooKeeperServer) {
        this.zooKeeperServer = zooKeeperServer;
    }

    @Override
    public StreamObserver<RpcRequest> process(StreamObserver<RpcResponse> responseObserver) {
        return new StreamObserver<RpcRequest>() {
            @Override
            public void onNext(RpcRequest request) {
                processRequest(request, responseObserver);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

    private void processRequest(RpcRequest request, StreamObserver<RpcResponse> responseObserver) {
        CallContext callContext = CallContext.KEY.get();
        if (request.hasConnect()) {
            processConnect(request.getConnect(), callContext, responseObserver);
        } else {
            RpcServerCnxn serverCnxn = connections.get(callContext.getClientAddress());
            if (request.hasCreate()) {
                processCreate(request.getHeader(), request.getCreate(), serverCnxn);
            } else if (request.hasGetData()) {
                serverCnxn.submitRequest(getData, request.getHeader().getXid(), boa -> serialize(boa, toGetDataRequest(request.getGetData())));
            } else if (request.hasSetData()) {
                serverCnxn.submitRequest(setData, request.getHeader().getXid(), boa -> serialize(boa, toSetDataRequest(request.getSetData())));
            } else if (request.hasGetChildren()) {
                serverCnxn.submitRequest(getChildren2, request.getHeader().getXid(), boa -> serialize(boa, toGetChildrenRequest(request.getGetChildren())));
            } else if (request.hasDelete()) {
                serverCnxn.submitRequest(delete, request.getHeader().getXid(), boa -> serialize(boa, toDeleteRequest(request.getDelete())));
            } else if (request.hasAuth()) {
                serverCnxn.submitRequest(auth, request.getHeader().getXid(), boa -> serialize(boa, toAuthInfo(request.getAuth())));
            } else if (request.hasExists()) {
                serverCnxn.submitRequest(exists, request.getHeader().getXid(), boa -> serialize(boa, toExistsRequest(request.getExists())));
            } else if (request.hasGetAcl()) {
                serverCnxn.submitRequest(getACL, request.getHeader().getXid(), boa -> serialize(boa, toGetAclRequest(request.getGetAcl())));
            } else if (request.hasSetAcl()) {
                serverCnxn.submitRequest(setACL, request.getHeader().getXid(), boa -> serialize(boa, toSetAclRequest(request.getSetAcl())));
            } else if (request.hasGetAllChildrenNumber()) {
                serverCnxn.submitRequest(getAllChildrenNumber, request.getHeader().getXid(), boa -> serialize(boa, toGetAllChildrenNumberRequest(request.getGetAllChildrenNumber())));
            } else if (request.hasGetEphemerals()) {
                serverCnxn.submitRequest(getEphemerals, request.getHeader().getXid(), boa -> serialize(boa, toGetEphemeralsRequest(request.getGetEphemerals())));
            } else if (request.hasAddWatch()) {
                serverCnxn.submitRequest(addWatch, request.getHeader().getXid(), boa -> serialize(boa, toAddWatchRequest(request.getAddWatch())));
            } else if (request.hasSync()) {
                serverCnxn.submitRequest(sync, request.getHeader().getXid(), boa -> serialize(boa, toSyncRequest(request.getSync())));
            }
        }
    }

    private void processCreate(RpcRequestHeader header, RpcCreateRequest request, RpcServerCnxn serverCnxn) {
        if ((request.getMode() == RpcCreateMode.PersistentSequentialWithTtl) || (request.getMode() == RpcCreateMode.PersistentWithTtl)) {
            serverCnxn.submitRequest(createTTL, header.getXid(), boa -> serialize(boa, toCreateTTLRequest(request)));
        } else {
            int opcode = (request.getMode() == RpcCreateMode.Container) ?  ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create2;
            serverCnxn.submitRequest(opcode, header.getXid(), boa -> serialize(boa, toCreateRequest(request)));
        }
    }

    private void processConnect(RpcConnectRequest request, CallContext callContext, StreamObserver<RpcResponse> responseObserver) {
        ConnectRequest connectRequest = toConnectRequest(request);
        RpcServerCnxn serverCnxn = new RpcServerCnxn(zooKeeperServer, callContext.getClientAddress(), responseObserver);
        try {
            connections.put(callContext.getClientAddress(), serverCnxn);
            zooKeeperServer.processConnectRequest(serverCnxn, connectRequest, request.getReadOnly());
        } catch (Exception e) {
            connections.remove(callContext.getClientAddress());
            responseObserver.onError(e);
        }
    }

    private void serialize(BinaryOutputArchive boa, Record record) {
        try {
            record.serialize(boa, "record");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
