package org.apache.zookeeper.rpc;

import org.apache.jute.Record;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.rpc.generated.v1.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.zookeeper.ZooDefs.OpCode.createTTL;
import static org.apache.zookeeper.rpc.Mapper.*;
import static org.apache.zookeeper.rpc.generated.v1.RpcCreateMode.*;
import static org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode.CODE_OK;
import static org.apache.zookeeper.rpc.generated.v1.RpcRequest.MessageCase.*;

class RequestMapper<T> {
    private static final Map<RpcRequest.MessageCase, RequestMapper<?>> map;
    private final InternalHandler<T> handler;
    private final Function<RpcRequest, T> accessor;

    private interface InternalHandler<T> {
        void handle(Chroot chroot, T request, long xid, RpcServerConnection connection);
    }

    private RequestMapper(InternalHandler<T> handler, Function<RpcRequest, T> accessor) {
        this.handler = handler;
        this.accessor = accessor;
    }

    private static <T> RequestMapper<T> build(InternalHandler<T> handler, Function<RpcRequest, T> accessor) {
        return new RequestMapper<>(handler, accessor);
    }

    static {
        Map<RpcRequest.MessageCase, RequestMapper<?>> work = new HashMap<>();
        work.put(CONNECT, build(RequestMapper::connectRequest, RpcRequest::getConnect));
        work.put(CREATE, build(RequestMapper::createRequest, RpcRequest::getCreate));
        work.put(DELETE, build(RequestMapper::deleteRequest, RpcRequest::getDelete));
        work.put(EXISTS, build(RequestMapper::existsRequest, RpcRequest::getExists));
        work.put(GETDATA, build(RequestMapper::getDataRequest, RpcRequest::getGetData));
        work.put(SETDATA, build(RequestMapper::setDataRequest, RpcRequest::getSetData));
        work.put(GETACL, build(RequestMapper::getACLRequest, RpcRequest::getGetACL));
        work.put(SETACL, build(RequestMapper::setACLRequest, RpcRequest::getSetACL));
        work.put(GETCHILDREN, build(RequestMapper::getChildrenRequest, RpcRequest::getGetChildren));
        work.put(GETALLCHILDRENNUMBER, build(RequestMapper::getAllChildrenNumberRequest, RpcRequest::getGetAllChildrenNumber));
        work.put(GETEPHEMERALS, build(RequestMapper::getEphemeralsRequest, RpcRequest::getGetEphemerals));
        work.put(REMOVEALLWATCHES, build(RequestMapper::removeAllWatchesRequest, RpcRequest::getRemoveAllWatches));
        work.put(ADDWATCH, build(RequestMapper::addWatchRequest, RpcRequest::getAddWatch));
        work.put(FOURLETTER, build(RequestMapper::fourLetterRequest, RpcRequest::getFourLetter));
        work.put(FOURLETTERSETTRACEMASK, build(RequestMapper::fourLetterSetTraceMaskRequest, RpcRequest::getFourLetterSetTraceMask));
        work.put(TRANSACTION, build(RequestMapper::transactionRequest, RpcRequest::getTransaction));
        work.put(GETCONFIG, build(RequestMapper::getConfigRequest, RpcRequest::getGetConfig));
        work.put(GETSERVERMETADATA, build(RequestMapper::getServerMetaDataRequest, RpcRequest::getGetServerMetaData));
        work.put(REREGISTERWATCHERS, build(RequestMapper::reRegisterWatchersRequest, RpcRequest::getReRegisterWatchers));
        work.put(CLOSESESSION, build(RequestMapper::closeSessionRequest, __ -> null));
        work.put(MESSAGE_NOT_SET, build(RequestMapper::pingRequest, __ -> null));
        map = Collections.unmodifiableMap(work);
    }

    static RequestMapper<?> get(RpcRequest request) {
        RequestMapper<?> requestMapper = map.get(request.getMessageCase());
        if (requestMapper == null) {
            throw new IllegalArgumentException("No mapper found for: " + request.getMessageCase());
        }
        return requestMapper;
    }

    void handle(Chroot chroot, RpcRequest request, RpcServerConnection connection) {
        handler.handle(chroot, accessor.apply(request), request.getXid(), connection);
    }

    private static void connectRequest(Chroot chroot, RpcConnectRequest request, long xid, RpcServerConnection connection) {
        ConnectRequest connectRequest = new ConnectRequest(0,
                request.getLastZxidSeen(),
                request.getSessionTimeOut(),
                request.getSessionId(),
                request.getPassword().toByteArray()
        );
        connection.processConnectRequest(connectRequest, request.getCanBeReadOnly(), request.getChrootPath());
    }

    private static void createRequest(Chroot chroot, RpcCreateRequest request, long xid, RpcServerConnection connection) {
        Record record;
        int opcode;
        if ((request.getCreateMode() == CREATE_MODE_PERSISTENT_WITH_TTL) || (request.getCreateMode() == CREATE_MODE_PERSISTENT_SEQUENTIAL_WITH_TTL)) {
            opcode = createTTL;
            record = new CreateTTLRequest(
                    chroot.fixPath(request.getPath()),
                    request.getData().toByteArray(),
                    toAcls(request.getACLs()),
                    toCreateMode(request.getCreateMode()).toFlag(),
                    request.getTtl()
            );
        } else {
            opcode = (request.getCreateMode() == CREATE_MODE_CONTAINER) ?  ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create2;
            record = new CreateRequest(
                    chroot.fixPath(request.getPath()),
                    request.getData().toByteArray(),
                    toAcls(request.getACLs()),
                    toCreateMode(request.getCreateMode()).toFlag()
            );
        }
        connection.processPacket(opcode, xid, record);
    }

    private static void deleteRequest(Chroot chroot, RpcDeleteRequest request, long xid, RpcServerConnection connection) {
        DeleteRequest record = new DeleteRequest(chroot.fixPath(request.getPath()), request.getVersion());
        connection.processPacket(ZooDefs.OpCode.delete, xid, record);
    }

    private static void existsRequest(Chroot chroot, RpcExistsRequest request, long xid, RpcServerConnection connection) {
        ExistsRequest record = new ExistsRequest(chroot.fixPath(request.getPath()), request.getWatch());
        connection.processPacket(ZooDefs.OpCode.exists, xid, record);
    }

    private static void getDataRequest(Chroot chroot, RpcGetDataRequest request, long xid, RpcServerConnection connection) {
        GetDataRequest record = new GetDataRequest(chroot.fixPath(request.getPath()), request.getWatch());
        connection.processPacket(ZooDefs.OpCode.getData, xid, record);
    }

    private static void setDataRequest(Chroot chroot, RpcSetDataRequest request, long xid, RpcServerConnection connection) {
        SetDataRequest record = new SetDataRequest(chroot.fixPath(request.getPath()), request.getData().toByteArray(), request.getVersion());
        connection.processPacket(ZooDefs.OpCode.setData, xid, record);
    }

    private static void getACLRequest(Chroot chroot, RpcGetACLRequest request, long xid, RpcServerConnection connection) {
        GetACLRequest record = new GetACLRequest(chroot.fixPath(request.getPath()));
        connection.processPacket(ZooDefs.OpCode.getACL, xid, record);
    }

    private static void setACLRequest(Chroot chroot, RpcSetACLRequest request, long xid, RpcServerConnection connection) {
        SetACLRequest record = new SetACLRequest(chroot.fixPath(request.getPath()), toAcls(request.getACLs()), request.getAclVersion());
        connection.processPacket(ZooDefs.OpCode.setACL, xid, record);
    }

    private static void getChildrenRequest(Chroot chroot, RpcGetChildrenRequest request, long xid, RpcServerConnection connection) {
        GetChildren2Request record = new GetChildren2Request(chroot.fixPath(request.getPath()), request.getWatch());
        connection.processPacket(ZooDefs.OpCode.getChildren2, xid, record);
    }

    private static void getAllChildrenNumberRequest(Chroot chroot, RpcGetAllChildrenNumberRequest request, long xid, RpcServerConnection connection) {
        GetAllChildrenNumberRequest record = new GetAllChildrenNumberRequest(chroot.fixPath(request.getPath()));
        connection.processPacket(ZooDefs.OpCode.getAllChildrenNumber, xid, record);
    }

    private static void getEphemeralsRequest(Chroot chroot, RpcGetEphemeralsRequest request, long xid, RpcServerConnection connection) {
        GetEphemeralsRequest record = new GetEphemeralsRequest(chroot.fixPath(request.getPrefixPath()));
        connection.processPacket(ZooDefs.OpCode.getEphemerals, xid, record);
    }

    private static void removeAllWatchesRequest(Chroot chroot, RpcRemoveAllWatchesRequest request, long xid, RpcServerConnection connection) {
        RemoveWatchesRequest record = new RemoveWatchesRequest(chroot.fixPath(request.getPath()), toWatcherType(request.getWatcherType()));
        connection.processPacket(ZooDefs.OpCode.removeWatches, xid, record);
    }

    private static void addWatchRequest(Chroot chroot, RpcAddWatchRequest request, long xid, RpcServerConnection connection) {
        AddWatchRequest record = new AddWatchRequest(chroot.fixPath(request.getBasePath()), toAddWatchMode(request.getMode()));
        connection.processPacket(ZooDefs.OpCode.addWatch, xid, record);
    }

    private static void transactionRequest(Chroot chroot, RpcTransactionRequest request, long xid, RpcServerConnection connection) {
        List<Op> operations = request.getOperationsList().stream()
                .map(o -> Mapper.toOperation(chroot, o))
                .collect(Collectors.toList());
        MultiOperationRecord record = new MultiOperationRecord(operations);
        int opcode = (record.getOpKind() == Op.OpKind.TRANSACTION) ? ZooDefs.OpCode.multi : ZooDefs.OpCode.multiRead;
        connection.processPacket(opcode, xid, record);
    }

    private static void getConfigRequest(Chroot chroot, RpcGetConfigRequest request, long xid, RpcServerConnection connection) {
        GetDataRequest record = new GetDataRequest(ZooDefs.CONFIG_NODE, request.getWatch());
        connection.processPacket(ZooDefs.OpCode.getData, xid, record);
    }

    private static void fourLetterRequest(Chroot chroot, RpcFourLetterRequest request, long xid, RpcServerConnection connection) {
        connection.fourLetterWord(request.getWord(), 0, false);
    }

    private static void fourLetterSetTraceMaskRequest(Chroot chroot, RpcFourLetterSetTraceMaskRequest request, long xid, RpcServerConnection connection) {
        connection.fourLetterWord("stmk", request.getTraceMask(), true);
    }

    private static void getServerMetaDataRequest(Chroot chroot, RpcGetServerMetaDataRequest request, long xid, RpcServerConnection connection) {
        Map<Integer, String> supportedMessages = Arrays.stream(RpcRequest.MessageCase.values())
                .map(m -> new AbstractMap.SimpleEntry<>(m.getNumber(), m.name()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        RpcGetServerMetaDataResponse response = RpcGetServerMetaDataResponse.newBuilder()
                .setVersion(1)
                .setMinXid(1)
                .setMaxXid(Integer.MAX_VALUE)
                .setMinDataVersion(0)
                .setMaxDataVersion(Integer.MAX_VALUE)
                .putAllSupportedMessages(supportedMessages)
                .build();
        connection.sendRpcResponse(xid, 0, CODE_OK, response);
    }

    private static void pingRequest(Chroot chroot, Object dummy, long dummyXid, RpcServerConnection connection) {
        connection.processPacket(ZooDefs.OpCode.ping, ClientCnxn.PING_XID, null);
    }

    private static void reRegisterWatchersRequest(Chroot chroot, RpcReRegisterWatchersRequest request, long xid, RpcServerConnection connection) {
        SetWatches2 record = new SetWatches2(request.getLastZxid(), request.getDataWatchesList(), request.getExistWatchesList(),
                request.getChildWatchesList(), request.getPersistentWatchesList(), request.getPersistentRecursiveWatchesList());
        connection.processPacket(ZooDefs.OpCode.setWatches2, ClientCnxn.SET_WATCHES_XID, record);
    }

    private static void closeSessionRequest(Chroot chroot, Object dummy, long xid, RpcServerConnection connection) {
        connection.processPacket(ZooDefs.OpCode.closeSession, 0, null);
    }
}
