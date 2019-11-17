package org.apache.zookeeper.grpc;

import static org.apache.zookeeper.grpc.RpcMappers.fromConnectResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromCreateResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromError;
import static org.apache.zookeeper.grpc.RpcMappers.fromErrorResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromGetAclResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromGetAllChildrenNumberResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromGetChildrenResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromGetDataResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromGetEphemeralsResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromSetAclResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromSetDataResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromSyncResponse;
import static org.apache.zookeeper.grpc.RpcMappers.fromWatcherEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.function.Consumer;
import io.grpc.stub.StreamObserver;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.grpc.generated.RpcKeeperException;
import org.apache.zookeeper.grpc.generated.RpcResponse;
import org.apache.zookeeper.grpc.generated.RpcResponseHeader;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetAllChildrenNumberResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetEphemeralsResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooKeeperServer;

public class RpcServerCnxn extends ServerCnxn {
    private final InetSocketAddress clientAddress;
    private final StreamObserver<RpcResponse> responseObserver;
    private volatile long sessionId;
    private volatile int sessionTimeout;

    public RpcServerCnxn(ZooKeeperServer zkServer, InetSocketAddress clientAddress, StreamObserver<RpcResponse> responseObserver) {
        super(zkServer);
        this.clientAddress = clientAddress;
        this.responseObserver = responseObserver;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public void close(DisconnectReason reason) {

    }

    void submitRequest(int opcode, int xid, Consumer<BinaryOutputArchive> serializeProc) {
        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            RequestHeader requestHeader = new RequestHeader(xid, opcode);
            requestHeader.serialize(boa, "header");
            serializeProc.accept(boa);
            baos.close();
            ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
            bb.rewind();
            getZooKeeperServer().processPacket(this, bb);
        } catch (IOException e) {
            responseObserver.onError(e);    // TODO
        }
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag, String cacheKey, Stat stat) {
        RpcKeeperException rpcKeeperException;
        if (h.getErr() != 0) {
            rpcKeeperException = fromError(KeeperException.Code.get(h.getErr()));
        } else {
            rpcKeeperException = RpcKeeperException.Ok;
        }
        RpcResponseHeader header = RpcResponseHeader.newBuilder().setXid(h.getXid()).setZxid(h.getZxid()).setException(rpcKeeperException).build();
        RpcResponse.Builder builder = RpcResponse.newBuilder().setHeader(header);
        // TODO
        if (r instanceof Create2Response) {
            builder.setCreate(fromCreateResponse((Create2Response) r));
        }
        else if (r instanceof GetDataResponse) {
            builder.setGetData(fromGetDataResponse((GetDataResponse) r));
        }
        else if (r instanceof SetDataResponse) {
            builder.setSetData(fromSetDataResponse((SetDataResponse) r));
        }
        else if (r instanceof WatcherEvent) {
            builder.setWatchedEvent(fromWatcherEvent((WatcherEvent) r));
        }
        else if (r instanceof GetChildren2Response) {
            builder.setGetChildren(fromGetChildrenResponse((GetChildren2Response) r));
        }
        else if (r instanceof GetACLResponse) {
            builder.setGetAcl(fromGetAclResponse((GetACLResponse) r));
        }
        else if (r instanceof SetACLResponse) {
            builder.setSetAcl(fromSetAclResponse((SetACLResponse) r));
        }
        else if (r instanceof GetAllChildrenNumberResponse) {
            builder.setGetAllChildrenNumber(fromGetAllChildrenNumberResponse((GetAllChildrenNumberResponse) r));
        }
        else if (r instanceof GetEphemeralsResponse) {
            builder.setGetEphemerals(fromGetEphemeralsResponse((GetEphemeralsResponse) r));
        }
        else if (r instanceof ErrorResponse) {
            builder.setError(fromErrorResponse((ErrorResponse) r));
        }
        else if (r instanceof SyncResponse) {
            builder.setSync(fromSyncResponse((SyncResponse) r));
        }
        responseObserver.onNext(builder.build());
    }

    @Override
    public void sendCloseSession() {

    }

    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
/*
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                    LOG,
                    ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "Deliver event " + event + " to 0x" + Long.toHexString(this.sessionId) + " through " + this);
        }
*/

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();

        try {
            sendResponse(h, e, "notification");
        } catch (IOException e1) {
            //LOG.debug("Problem sending to {}", getRemoteSocketAddress(), e1);
            close(DisconnectReason.UNKNOWN);
        }
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public void sendBuffer(ByteBuffer... buffers) {
        for ( ByteBuffer buffer : buffers ) {
            try {
                if (buffer.capacity() == 0) {
                    // ServerCnxnFactory.closeConn
                } else {
                    ByteBufferInputStream bbis = new ByteBufferInputStream(buffer);
                    BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
                    bbia.readInt("len");
                    ConnectResponse connectResponse = new ConnectResponse();
                    connectResponse.deserialize(bbia, "connect");
                    // TODO                 bos.writeBool(this instanceof ReadOnlyZooKeeperServer, "readOnly");
                    responseObserver.onNext(RpcResponse.newBuilder().setConnect(fromConnectResponse(connectResponse)).build());
                }
            } catch (IOException e) {
                responseObserver.onError(e);    // TODO
            }
        }
    }

    @Override
    public void enableRecv() {

    }

    @Override
    public void disableRecv(boolean waitDisableRecv) {

    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    protected ServerStats serverStats() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return clientAddress;
    }

    @Override
    public int getInterestOps() {
        return 0;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        return new Certificate[0];
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {

    }
}
