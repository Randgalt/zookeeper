package org.apache.zookeeper.rpc;

import static org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode.CODE_OK;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import com.google.protobuf.ByteString;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.rpc.generated.v1.RpcConnectResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcFourLetterResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcFourLetterSetTraceMaskResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;
import org.apache.zookeeper.rpc.generated.v1.RpcRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;

class RpcServerConnection extends ServerCnxn implements StreamObserver<RpcRequest> {
    private final RpcServerConnectionFactory factory;
    private final RpcZooKeeperService rpcZooKeeperService;
    private final ZooKeeperServer zooKeeperServer;
    private final StreamObserver<RpcResponse> responseStream;
    private final InetSocketAddress clientAddress;
    private volatile Chroot chroot = new Chroot(null);
    private volatile int sessionTimeout = 0;
    private volatile long sessionId = 0;

    RpcServerConnection(RpcServerConnectionFactory factory,
                        RpcZooKeeperService rpcZooKeeperService,
                        ZooKeeperServer zooKeeperServer,
                        StreamObserver<RpcResponse> responseStream,
                        InetSocketAddress clientAddress) {
        super(zooKeeperServer);
        this.factory = factory;
        this.rpcZooKeeperService = rpcZooKeeperService;
        this.zooKeeperServer = zooKeeperServer;
        this.responseStream = responseStream;
        this.clientAddress = clientAddress;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public void close(DisconnectReason reason) {
        disconnectReason = reason;
        close();
    }

    public void close() {
        setStale();
        rpcZooKeeperService.remove(this);
        factory.removeCnxnFromSessionMap(this);
        zooKeeperServer.removeCnxn(this);
        responseStream.onCompleted();
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag, String cacheKey, Stat stat, int opCode) {
        Object response;
        int errorCode;
        if (r instanceof ErrorResponse) {
            errorCode = ((ErrorResponse) r).getErr();
            r = null;
        } else {
            errorCode = h.getErr();
        }

        if (r != null) {
            if ((opCode == ZooDefs.OpCode.getData) && ZooDefs.CONFIG_NODE.equals(cacheKey)) {
                response = ResponseMapper.toConfigData((GetDataResponse)r);
            } else {
                response = ResponseMapper.toResponse(chroot, r);
            }
            sendRpcResponse(h.getXid(), h.getZxid(), Mapper.fromCode(errorCode), response);
        } else {
            sendRpcResponse(h.getXid(), h.getZxid(), Mapper.fromCode(errorCode), null);
        }
    }

    <T> void sendRpcResponse(long xid, long zid, RpcKeeperExceptionCode code, T response) {
        if (response == null) {
            sendToClient(RpcResponseBuilder.build(xid, zid, code));
        } else {
            sendToClient(RpcResponseBuilder.build(xid, zid, code, response));
        }
    }

    @Override
    public void sendCloseSession() {
        close(DisconnectReason.CLIENT_CLOSED_CONNECTION);
    }

    @Override
    public void process(WatchedEvent event) {
        sendToClient(RpcResponseBuilder.build(0, 0, CODE_OK, Mapper.fromWatchedEvent(chroot, event)));
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void sendConnectResponse(boolean valid, byte[] password, boolean isReadOnly) {
        RpcConnectResponse response = RpcConnectResponse.newBuilder()
                .setPassword(ByteString.copyFrom(password))
                .setSessionId(getSessionId())
                .setReadOnly(isReadOnly)
                .setSessionTimeOut(sessionTimeout)
                .build();
        sendRpcResponse(0, 0, CODE_OK, response);
    }

    @Override
    public void sendBuffer(ByteBuffer... buffers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendCloseConnection() {
        close(DisconnectReason.CLIENT_CLOSED_CONNECTION);
    }

    @Override
    public void enableRecv() {
        // TODO
    }

    @Override
    public void disableRecv(boolean waitDisableRecv) {
        // TODO
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    protected ServerStats serverStats() {
        if (zooKeeperServer == null) {
            return null;
        }
        return zooKeeperServer.serverStats();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return clientAddress;
    }

    @Override
    public int getInterestOps() {
        return 0;   // TODO
    }

    @Override
    public boolean isSecure() {
        return false;   // TODO
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        return new Certificate[0];  // TODO
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        // TODO
    }

    public Chroot getChroot() {
        return chroot;
    }

    void processConnectRequest(ConnectRequest request, boolean canBeReadOnly, String chrootPath) {
        chroot = new Chroot(chrootPath);
        try {
            zooKeeperServer.processConnectRequest(this, request, canBeReadOnly);
        } catch (Exception e) {
            // TODO
        }
    }

    void processPacket(int opcode, long xid, Record record) {
        try {
            if (xid > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("xid is over Integer.MAX_VALUE: " + xid);
            }
            RequestHeader requestHeader = new RequestHeader((int)xid, opcode);
            RpcRequestPacket packet = new RpcRequestPacket(requestHeader, record);
            zooKeeperServer.processPacket(this, packet);
        } catch (IOException e) {
            // TODO
        }
    }

    @Override
    public void onNext(RpcRequest request) {
        RequestMapper<?> requestMapper = RequestMapper.get(request);
        requestMapper.handle(chroot, request, this);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
        // TODO
    }

    @Override
    public void onCompleted() {
        close(DisconnectReason.CLIENT_CLOSED_CONNECTION);    // TODO
    }

    public void fourLetterWord(String word, long traceMask, boolean isSetTraceMask) {
        Writer writer = new Writer() {
            private final StringBuffer result = new StringBuffer();

            @Override
            public void write(char[] cbuf, int off, int len) {
                result.append(cbuf, off, len);
            }

            @Override
            public void flush() {
                // NOP
            }

            @Override
            public void close() {
                sendFourLetterResult(result.toString(), isSetTraceMask);
            }
        };
        PrintWriter printWriter = new PrintWriter(writer);

        String error = null;
        int cmd = ByteBuffer.wrap(word.getBytes()).getInt();
        if ((word.length() == 4) && FourLetterCommands.isKnown(cmd)) {
            if (FourLetterCommands.isEnabled(word)) {
                if (isSetTraceMask) {
                    ZooTrace.setTextTraceLevel(traceMask);
                    SetTraceMaskCommand setMask = new SetTraceMaskCommand(printWriter, this, traceMask);
                    setMask.start();
                } else {
                    CommandExecutor commandExecutor = new CommandExecutor();
                    commandExecutor.execute(this, printWriter, cmd, zooKeeperServer, zooKeeperServer.getServerCnxnFactory());
                }
            } else {
                error = word + " is not executed because it is not in the whitelist.";
            }
        } else {
            error = "Unknown command: " + word;
        }

        if (error != null) {
            new NopCommand(printWriter, this, error).start();
        }
    }

    private void sendFourLetterResult(String result, boolean isSetTraceMask) {
        if (isSetTraceMask) {
            RpcFourLetterSetTraceMaskResponse response = RpcFourLetterSetTraceMaskResponse.newBuilder().setResult(result).build();
            sendRpcResponse(0, 0, CODE_OK, response);
        } else {
            RpcFourLetterResponse response = RpcFourLetterResponse.newBuilder().setResult(result).build();
            sendRpcResponse(0, 0, CODE_OK, response);
        }
    }

    private void sendToClient(RpcResponse response) {
        if (!isStale() && !isInvalid()) {
            responseStream.onNext(response);
        }
    }
}
