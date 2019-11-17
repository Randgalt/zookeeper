package org.apache.zookeeper.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.protobuf.ByteString;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.grpc.generated.RpcAcl;
import org.apache.zookeeper.grpc.generated.RpcAddWatchMode;
import org.apache.zookeeper.grpc.generated.RpcAddWatchRequest;
import org.apache.zookeeper.grpc.generated.RpcAuthInfo;
import org.apache.zookeeper.grpc.generated.RpcConnectRequest;
import org.apache.zookeeper.grpc.generated.RpcConnectResponse;
import org.apache.zookeeper.grpc.generated.RpcCreateMode;
import org.apache.zookeeper.grpc.generated.RpcCreateRequest;
import org.apache.zookeeper.grpc.generated.RpcCreateResponse;
import org.apache.zookeeper.grpc.generated.RpcDeleteRequest;
import org.apache.zookeeper.grpc.generated.RpcErrorResponse;
import org.apache.zookeeper.grpc.generated.RpcEventType;
import org.apache.zookeeper.grpc.generated.RpcExistsRequest;
import org.apache.zookeeper.grpc.generated.RpcGetAclRequest;
import org.apache.zookeeper.grpc.generated.RpcGetAclResponse;
import org.apache.zookeeper.grpc.generated.RpcGetAllChildrenNumberRequest;
import org.apache.zookeeper.grpc.generated.RpcGetAllChildrenNumberResponse;
import org.apache.zookeeper.grpc.generated.RpcGetChildrenRequest;
import org.apache.zookeeper.grpc.generated.RpcGetChildrenResponse;
import org.apache.zookeeper.grpc.generated.RpcGetDataRequest;
import org.apache.zookeeper.grpc.generated.RpcGetDataResponse;
import org.apache.zookeeper.grpc.generated.RpcGetEphemeralsRequest;
import org.apache.zookeeper.grpc.generated.RpcGetEphemeralsResponse;
import org.apache.zookeeper.grpc.generated.RpcId;
import org.apache.zookeeper.grpc.generated.RpcKeeperException;
import org.apache.zookeeper.grpc.generated.RpcKeeperState;
import org.apache.zookeeper.grpc.generated.RpcPerms;
import org.apache.zookeeper.grpc.generated.RpcSetAclRequest;
import org.apache.zookeeper.grpc.generated.RpcSetAclResponse;
import org.apache.zookeeper.grpc.generated.RpcSetDataRequest;
import org.apache.zookeeper.grpc.generated.RpcSetDataResponse;
import org.apache.zookeeper.grpc.generated.RpcStat;
import org.apache.zookeeper.grpc.generated.RpcSyncRequest;
import org.apache.zookeeper.grpc.generated.RpcSyncResponse;
import org.apache.zookeeper.grpc.generated.RpcWatchedEvent;
import org.apache.zookeeper.proto.AddWatchRequest;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetAllChildrenNumberRequest;
import org.apache.zookeeper.proto.GetAllChildrenNumberResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetEphemeralsRequest;
import org.apache.zookeeper.proto.GetEphemeralsResponse;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.proto.WatcherEvent;

@SuppressWarnings("deprecation")
public class RpcMappers {
    private static final Map<KeeperException.Code, RpcKeeperException> errorMap;

    static {
        Map<KeeperException.Code, RpcKeeperException> errorWork = new HashMap<>();
        errorWork.put(KeeperException.Code.OK, RpcKeeperException.Ok);
        errorWork.put(KeeperException.Code.SYSTEMERROR, RpcKeeperException.SystemError);
        errorWork.put(KeeperException.Code.RUNTIMEINCONSISTENCY, RpcKeeperException.RuntimeInconsistencyError);
        errorWork.put(KeeperException.Code.DATAINCONSISTENCY, RpcKeeperException.DataInconsistencyError);
        errorWork.put(KeeperException.Code.CONNECTIONLOSS, RpcKeeperException.ConnectionLossError);
        errorWork.put(KeeperException.Code.MARSHALLINGERROR, RpcKeeperException.MarshallingError);
        errorWork.put(KeeperException.Code.UNIMPLEMENTED, RpcKeeperException.UnimplementedError);
        errorWork.put(KeeperException.Code.OPERATIONTIMEOUT, RpcKeeperException.OperationTimeoutError);
        errorWork.put(KeeperException.Code.BADARGUMENTS, RpcKeeperException.BadArgumentsError);
        errorWork.put(KeeperException.Code.NEWCONFIGNOQUORUM, RpcKeeperException.NewConfigNoQuorumError);
        errorWork.put(KeeperException.Code.RECONFIGINPROGRESS, RpcKeeperException.ReconfigInProgressError);
        errorWork.put(KeeperException.Code.UNKNOWNSESSION, RpcKeeperException.UnknownSessionError);
        errorWork.put(KeeperException.Code.APIERROR, RpcKeeperException.ApiError);
        errorWork.put(KeeperException.Code.NONODE, RpcKeeperException.NoNodeError);
        errorWork.put(KeeperException.Code.NOAUTH, RpcKeeperException.NoAuthError);
        errorWork.put(KeeperException.Code.BADVERSION, RpcKeeperException.BadVersionError);
        errorWork.put(KeeperException.Code.NOCHILDRENFOREPHEMERALS, RpcKeeperException.NoChildrenForEphemeralsError);
        errorWork.put(KeeperException.Code.NODEEXISTS, RpcKeeperException.NodeExistsError);
        errorWork.put(KeeperException.Code.NOTEMPTY, RpcKeeperException.NotEmptyError);
        errorWork.put(KeeperException.Code.SESSIONEXPIRED, RpcKeeperException.SessionExpiredError);
        errorWork.put(KeeperException.Code.INVALIDCALLBACK, RpcKeeperException.InvalidCallbackError);
        errorWork.put(KeeperException.Code.INVALIDACL, RpcKeeperException.InvalidAclError);
        errorWork.put(KeeperException.Code.AUTHFAILED, RpcKeeperException.AuthFailedError);
        errorWork.put(KeeperException.Code.SESSIONMOVED, RpcKeeperException.SessionMovedError);
        errorWork.put(KeeperException.Code.NOTREADONLY, RpcKeeperException.NotReadonlyError);
        errorWork.put(KeeperException.Code.EPHEMERALONLOCALSESSION, RpcKeeperException.EphemeralOnLocalSessionError);
        errorWork.put(KeeperException.Code.NOWATCHER, RpcKeeperException.NoWatcherError);
        errorWork.put(KeeperException.Code.REQUESTTIMEOUT, RpcKeeperException.RequestTimeoutError);
        errorWork.put(KeeperException.Code.RECONFIGDISABLED, RpcKeeperException.ReconfigDisabledError);
        errorWork.put(KeeperException.Code.SESSIONCLOSEDREQUIRESASLAUTH, RpcKeeperException.SessionClosedRequireSaslAuthError);
        errorMap = Collections.unmodifiableMap(errorWork);
    }

    public static RpcWatchedEvent toWatchedEvent(WatchedEvent watchedEvent) {
        return RpcWatchedEvent.newBuilder()
                .setEventType(toEventType(watchedEvent.getType()))
                .setPath(watchedEvent.getPath())
                .setKeeperState(toKeeperState(watchedEvent.getState()))
                .build();
    }

    public static RpcKeeperState toKeeperState(int keeperState) {
        return toKeeperState(Watcher.Event.KeeperState.fromInt(keeperState));
    }

    public static RpcKeeperState toKeeperState(Watcher.Event.KeeperState keeperState) {
        switch (keeperState) {
            default:
            case Unknown:
            case NoSyncConnected:
                return RpcKeeperState.UNRECOGNIZED;
            case Disconnected:
                return RpcKeeperState.Disconnected;
            case SyncConnected:
                return RpcKeeperState.SyncConnected;
            case AuthFailed:
                return RpcKeeperState.AuthFailed;
            case ConnectedReadOnly:
                return RpcKeeperState.ConnectedReadOnly;
            case SaslAuthenticated:
                return RpcKeeperState.SaslAuthenticated;
            case Expired:
                return RpcKeeperState.Expired;
            case Closed:
                return RpcKeeperState.Closed;
        }
    }

    public static RpcEventType toEventType(int eventType) {
        return toEventType(Watcher.Event.EventType.fromInt(eventType));
    }

    public static RpcEventType toEventType(Watcher.Event.EventType eventType) {
        switch (eventType) {
            default:
            case None:
                return RpcEventType.None;
            case NodeCreated:
                return RpcEventType.NodeCreated;
            case NodeDeleted:
                return RpcEventType.NodeDeleted;
            case NodeDataChanged:
                return RpcEventType.NodeDataChanged;
            case NodeChildrenChanged:
                return RpcEventType.NodeChildrenChanged;
            case DataWatchRemoved:
                return RpcEventType.DataWatchRemoved;
            case ChildWatchRemoved:
                return RpcEventType.ChildWatchRemoved;
            case PersistentWatchRemoved:
                return RpcEventType.PersistentWatchRemoved;
        }
    }

    public static CreateRequest toCreateRequest(RpcCreateRequest request) {
        return new CreateRequest(
                request.getPath(),
                request.getData().toByteArray(),
                request.getAclList().stream().map(RpcMappers::toAcl).collect(Collectors.toList()),
                toCreateMode(request.getMode()).toFlag()
        );
    }

    public static CreateTTLRequest toCreateTTLRequest(RpcCreateRequest request) {
        return new CreateTTLRequest(
                request.getPath(),
                request.getData().toByteArray(),
                request.getAclList().stream().map(RpcMappers::toAcl).collect(Collectors.toList()),
                toCreateMode(request.getMode()).toFlag(),
                request.getTtl()
        );
    }

    public static RpcCreateResponse fromCreateResponse(Create2Response response) {
        return RpcCreateResponse.newBuilder().setPath(response.getPath()).setStat(fromStat(response.getStat())).build();
    }

    public static int toPerms(List<RpcPerms> perm) {
        return perm.stream().mapToInt(RpcMappers::toPerm).sum();
    }

    public static int toPerm(RpcPerms perm) {
        switch (perm) {
            default:
                return 0;
            case Read:
                return ZooDefs.Perms.READ;
            case Write:
                return ZooDefs.Perms.WRITE;
            case Create:
                return ZooDefs.Perms.CREATE;
            case Delete:
                return ZooDefs.Perms.DELETE;
            case Admin:
                return ZooDefs.Perms.ADMIN;
            case All:
                return ZooDefs.Perms.ALL;
        }
    }

    public static List<RpcPerms> fromPerms(int perms) {
        List<RpcPerms> list = new ArrayList<>();
        if ((perms & ZooDefs.Perms.READ) == ZooDefs.Perms.READ) {
            list.add(RpcPerms.Read);
        }
        if ((perms & ZooDefs.Perms.WRITE) == ZooDefs.Perms.WRITE) {
            list.add(RpcPerms.Write);
        }
        if ((perms & ZooDefs.Perms.CREATE) == ZooDefs.Perms.CREATE) {
            list.add(RpcPerms.Create);
        }
        if ((perms & ZooDefs.Perms.DELETE) == ZooDefs.Perms.DELETE) {
            list.add(RpcPerms.Delete);
        }
        if ((perms & ZooDefs.Perms.ADMIN) == ZooDefs.Perms.ADMIN) {
            list.add(RpcPerms.Admin);
        }
        if ((perms & ZooDefs.Perms.ALL) == ZooDefs.Perms.ALL) {
            list.add(RpcPerms.All);
        }
        return list;
    }

    public static List<ACL> toAcls(List<RpcAcl> acl) {
        return acl.stream().map(RpcMappers::toAcl).collect(Collectors.toList());
    }

    public static ACL toAcl(RpcAcl acl) {
        return new ACL(toPerms(acl.getPermsList()), toId(acl.getId()));
    }

    public static Id toId(RpcId id) {
        return new Id(id.getScheme(), id.getId());
    }

    public static ConnectRequest toConnectRequest(RpcConnectRequest request) {
        return new ConnectRequest(
                0,
                request.getLastZxidSeen(),
                request.getTimeOut(),
                request.getSessionId(),
                request.getPasswd().toByteArray()
        );
    }

    public static GetDataRequest toGetDataRequest(RpcGetDataRequest request) {
        return new GetDataRequest(request.getPath(), request.getWatch());
    }

    public static RpcGetDataResponse fromGetDataResponse(GetDataResponse response) {
        return RpcGetDataResponse.newBuilder().setData(ByteString.copyFrom(response.getData())).setStat(fromStat(response.getStat())).build();
    }

    public static SetDataRequest toSetDataRequest(RpcSetDataRequest request) {
        return new SetDataRequest(request.getPath(), request.getData().toByteArray(), request.getVersion());
    }

    public static RpcSetDataResponse fromSetDataResponse(SetDataResponse response) {
        return RpcSetDataResponse.newBuilder().setStat(fromStat(response.getStat())).build();
    }

    public static GetChildren2Request toGetChildrenRequest(RpcGetChildrenRequest request) {
        return new GetChildren2Request(request.getPath(), request.getWatch());
    }

    public static RpcGetChildrenResponse fromGetChildrenResponse(GetChildren2Response response) {
        return RpcGetChildrenResponse.newBuilder().addAllChildren(response.getChildren()).setStat(fromStat(response.getStat())).build();
    }

    public static DeleteRequest toDeleteRequest(RpcDeleteRequest request) {
        return new DeleteRequest(request.getPath(), request.getVersion());
    }

    public static AuthPacket toAuthInfo(RpcAuthInfo info) {
        return new AuthPacket(0, info.getScheme(), info.getAuth().toByteArray());
    }

    public static CreateMode toCreateMode(RpcCreateMode createMode) {
        switch (createMode) {
            default:
                throw new IllegalArgumentException("Unknown mode: " + createMode);
            case Persistent:
                return CreateMode.PERSISTENT;
            case PersistentSequential:
                return CreateMode.PERSISTENT_SEQUENTIAL;
            case Ephemeral:
                return CreateMode.EPHEMERAL;
            case EphemeralSequential:
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            case Container:
                return CreateMode.CONTAINER;
            case PersistentWithTtl:
                return CreateMode.PERSISTENT_WITH_TTL;
            case PersistentSequentialWithTtl:
                return CreateMode.PERSISTENT_WITH_TTL;
        }
    }

    public static GetACLRequest toGetAclRequest(RpcGetAclRequest request) {
        return new GetACLRequest(request.getPath());
    }

    public static RpcGetAclResponse fromGetAclResponse(GetACLResponse response) {
        return RpcGetAclResponse.newBuilder()
                .addAllAcl(fromAcls(response.getAcl()))
                .setStat(fromStat(response.getStat()))
                .build();
    }

    public static SetACLRequest toSetAclRequest(RpcSetAclRequest request) {
        return new SetACLRequest(request.getPath(), toAcls(request.getAclList()), request.getVersion());
    }

    public static RpcSetAclResponse fromSetAclResponse(SetACLResponse response) {
        return RpcSetAclResponse.newBuilder()
                .setStat(fromStat(response.getStat()))
                .build();
    }

    public static ExistsRequest toExistsRequest(RpcExistsRequest request) {
        return new ExistsRequest(request.getPath(), request.getWatch());
    }

    public static GetAllChildrenNumberRequest toGetAllChildrenNumberRequest(RpcGetAllChildrenNumberRequest request) {
        return new GetAllChildrenNumberRequest(request.getPath());
    }

    public static RpcGetAllChildrenNumberResponse fromGetAllChildrenNumberResponse(GetAllChildrenNumberResponse response) {
        return RpcGetAllChildrenNumberResponse.newBuilder()
                .setTotalNumber(response.getTotalNumber())
                .build();
    }

    public static GetEphemeralsRequest toGetEphemeralsRequest(RpcGetEphemeralsRequest request) {
        return new GetEphemeralsRequest(request.getPrefixPath());
    }

    public static RpcGetEphemeralsResponse fromGetEphemeralsResponse(GetEphemeralsResponse response) {
        return RpcGetEphemeralsResponse.newBuilder()
                .addAllEphemerals(response.getEphemerals())
                .build();
    }

    public static RpcWatchedEvent fromWatcherEvent(WatcherEvent event) {
        return RpcWatchedEvent.newBuilder()
                .setPath(event.getPath())
                .setKeeperState(toKeeperState(event.getState()))
                .setEventType(toEventType(event.getType()))
                .build();
    }

    public static RpcStat fromStat(Stat stat) {
        return RpcStat.newBuilder()
                .setAversion(stat.getAversion())
                .setCversion(stat.getCversion())
                .setVersion(stat.getVersion())
                .setCtime(stat.getCtime())
                .setMtime(stat.getMtime())
                .setCzxid(stat.getCzxid())
                .setEphemeralOwner(stat.getEphemeralOwner())
                .setMzxid(stat.getMzxid())
                .setPzxid(stat.getPzxid())
                .setDataLength(stat.getDataLength())
                .setNumChildren(stat.getNumChildren())
                .build();
    }

    public static RpcId fromId(Id id) {
        return RpcId.newBuilder().setScheme(id.getScheme()).setId(id.getId()).build();
    }

    public static RpcAcl fromAcl(ACL acl) {
        return RpcAcl.newBuilder().setId(fromId(acl.getId())).addAllPerms(fromPerms(acl.getPerms())).build();
    }

    public static List<RpcAcl> fromAcls(List<ACL> acls) {
        return acls.stream().map(RpcMappers::fromAcl).collect(Collectors.toList());
    }

    public static RpcConnectResponse fromConnectResponse(ConnectResponse connectResponse) {
        return RpcConnectResponse.newBuilder()
                .setSessionId(connectResponse.getSessionId())
                .setSessionTimeout(connectResponse.getTimeOut())
                .build();
    }

    public static RpcKeeperException fromError(KeeperException.Code code) {
        RpcKeeperException rpc = errorMap.get(code);
        if (rpc == null) {
            throw new IllegalArgumentException("No map for " + code);
        }
        return rpc;
    }

    public static RpcErrorResponse fromErrorResponse(ErrorResponse response) {
        return RpcErrorResponse.newBuilder()
                .setErr(response.getErr())
                .build();
    }

    public static int toAddWatchMode(RpcAddWatchMode mode) {
        switch (mode) {
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
            case PersistentWatch:
                return AddWatchMode.PERSISTENT.getMode();
            case PersistentRecursiveWatch:
                return AddWatchMode.PERSISTENT_RECURSIVE.getMode();
        }
    }

    public static SyncRequest toSyncRequest(RpcSyncRequest request) {
        return new SyncRequest(request.getPath());
    }

    public static RpcSyncResponse fromSyncResponse(SyncResponse response) {
        return RpcSyncResponse.newBuilder()
                .setPath(response.getPath())
                .build();
    }

    public static AddWatchRequest toAddWatchRequest(RpcAddWatchRequest request) {
        return new AddWatchRequest(request.getPath(), toAddWatchMode(request.getMode()));
    }

    private RpcMappers() {
    }
}
