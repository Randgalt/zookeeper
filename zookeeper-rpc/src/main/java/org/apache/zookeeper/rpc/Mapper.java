package org.apache.zookeeper.rpc;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.MultipleAddresses;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.rpc.generated.v1.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class Mapper {
    static CreateMode toCreateMode(RpcCreateMode createMode) {
        switch (createMode) {
            default:
                throw new IllegalArgumentException("Unknown mode: " + createMode);
            case CREATE_MODE_PERSISTENT:
                return CreateMode.PERSISTENT;
            case CREATE_MODE_PERSISTENT_SEQUENTIAL:
                return CreateMode.PERSISTENT_SEQUENTIAL;
            case CREATE_MODE_EPHEMERAL:
                return CreateMode.EPHEMERAL;
            case CREATE_MODE_EPHEMERAL_SEQUENTIAL:
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            case CREATE_MODE_CONTAINER:
                return CreateMode.CONTAINER;
            case CREATE_MODE_PERSISTENT_WITH_TTL:
                return CreateMode.PERSISTENT_WITH_TTL;
            case CREATE_MODE_PERSISTENT_SEQUENTIAL_WITH_TTL:
                return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;
        }
    }

    static List<ACL> toAcls(RpcACLs acls) {
        RpcZooDefsIds predefinedId = acls.getPredefinedId();
        switch (predefinedId) {
            case IDS_UNKNOWN:
                return acls.getAclsList().stream().map(Mapper::toAcl).collect(Collectors.toList());

            case IDS_OPEN_ACL_UNSAFE:
                return ZooDefs.Ids.OPEN_ACL_UNSAFE;

            case IDS_CREATOR_ALL_ACL:
                return ZooDefs.Ids.CREATOR_ALL_ACL;

            case IDS_READ_ACL_UNSAFE:
                return ZooDefs.Ids.READ_ACL_UNSAFE;

            default:
                throw new IllegalArgumentException("Unsupported predefined ACL: " + predefinedId);
        }
    }

    public static ACL toAcl(RpcACL acl) {
        return new ACL(toPerms(acl.getPermsList()), toId(acl.getId()));
    }

    static Id toId(RpcId id) {
        return new Id(id.getScheme(), id.getId());
    }

    static int toPerms(List<RpcZooDefsPerms> perm) {
        return perm.stream().mapToInt(Mapper::toPerm).sum();
    }

    static int toPerm(RpcZooDefsPerms perm) {
        switch (perm) {
            default:
                return 0;
            case PERMS_READ:
                return ZooDefs.Perms.READ;
            case PERMS_WRITE:
                return ZooDefs.Perms.WRITE;
            case PERMS_CREATE:
                return ZooDefs.Perms.CREATE;
            case PERMS_DELETE:
                return ZooDefs.Perms.DELETE;
            case PERMS_ADMIN:
                return ZooDefs.Perms.ADMIN;
            case PERMS_ALL:
                return ZooDefs.Perms.ALL;
        }
    }

    static RpcKeeperExceptionCode fromCode(int code) {
        switch (KeeperException.Code.get(code)) {
            default:
                return RpcKeeperExceptionCode.CODE_UNKNOWN;
            case OK:
                return RpcKeeperExceptionCode.CODE_OK;
            case SYSTEMERROR:
                return RpcKeeperExceptionCode.CODE_SYSTEMERROR;
            case RUNTIMEINCONSISTENCY:
                return RpcKeeperExceptionCode.CODE_RUNTIMEINCONSISTENCY;
            case DATAINCONSISTENCY:
                return RpcKeeperExceptionCode.CODE_DATAINCONSISTENCY;
            case CONNECTIONLOSS:
                return RpcKeeperExceptionCode.CODE_CONNECTIONLOSS;
            case MARSHALLINGERROR:
                return RpcKeeperExceptionCode.CODE_MARSHALLINGERROR;
            case UNIMPLEMENTED:
                return RpcKeeperExceptionCode.CODE_UNIMPLEMENTED;
            case OPERATIONTIMEOUT:
                return RpcKeeperExceptionCode.CODE_OPERATIONTIMEOUT;
            case BADARGUMENTS:
                return RpcKeeperExceptionCode.CODE_BADARGUMENTS;
            case NEWCONFIGNOQUORUM:
                return RpcKeeperExceptionCode.CODE_NEWCONFIGNOQUORUM;
            case RECONFIGINPROGRESS:
                return RpcKeeperExceptionCode.CODE_RECONFIGINPROGRESS;
            case UNKNOWNSESSION:
                return RpcKeeperExceptionCode.CODE_UNKNOWNSESSION;
            case APIERROR:
                return RpcKeeperExceptionCode.CODE_APIERROR;
            case NONODE:
                return RpcKeeperExceptionCode.CODE_NONODE;
            case NOAUTH:
                return RpcKeeperExceptionCode.CODE_NOAUTH;
            case BADVERSION:
                return RpcKeeperExceptionCode.CODE_BADVERSION;
            case NOCHILDRENFOREPHEMERALS:
                return RpcKeeperExceptionCode.CODE_NOCHILDRENFOREPHEMERALS;
            case NODEEXISTS:
                return RpcKeeperExceptionCode.CODE_NODEEXISTS;
            case NOTEMPTY:
                return RpcKeeperExceptionCode.CODE_NOTEMPTY;
            case SESSIONEXPIRED:
                return RpcKeeperExceptionCode.CODE_SESSIONEXPIRED;
            case INVALIDCALLBACK:
                return RpcKeeperExceptionCode.CODE_INVALIDCALLBACK;
            case INVALIDACL:
                return RpcKeeperExceptionCode.CODE_INVALIDACL;
            case AUTHFAILED:
                return RpcKeeperExceptionCode.CODE_AUTHFAILED;
            case SESSIONMOVED:
                return RpcKeeperExceptionCode.CODE_SESSIONMOVED;
            case NOTREADONLY:
                return RpcKeeperExceptionCode.CODE_NOTREADONLY;
            case EPHEMERALONLOCALSESSION:
                return RpcKeeperExceptionCode.CODE_EPHEMERALONLOCALSESSION;
            case NOWATCHER:
                return RpcKeeperExceptionCode.CODE_NOWATCHER;
            case REQUESTTIMEOUT:
                return RpcKeeperExceptionCode.CODE_REQUESTTIMEOUT;
            case RECONFIGDISABLED:
                return RpcKeeperExceptionCode.CODE_RECONFIGDISABLED;
            case SESSIONCLOSEDREQUIRESASLAUTH:
                return RpcKeeperExceptionCode.CODE_SESSIONCLOSEDREQUIRESASLAUTH;
        }
    }

    @SuppressWarnings("deprecation")
    static RpcKeeperState fromKeeperState(Watcher.Event.KeeperState keeperState) {
        switch (keeperState) {
            default:
            case Unknown:
                return RpcKeeperState.KEEPER_STATE_UNKNOWN;
            case NoSyncConnected:
                return RpcKeeperState.KEEPER_STATE_NO_SYNC_CONNECTED;
            case Disconnected:
                return RpcKeeperState.KEEPER_STATE_DISCONNECTED;
            case SyncConnected:
                return RpcKeeperState.KEEPER_STATE_SYNC_CONNECTED;
            case AuthFailed:
                return RpcKeeperState.KEEPER_STATE_AUTH_FAILED;
            case ConnectedReadOnly:
                return RpcKeeperState.KEEPER_STATE_CONNECTED_READ_ONLY;
            case SaslAuthenticated:
                return RpcKeeperState.KEEPER_STATE_SASL_AUTHENTICATED;
            case Expired:
                return RpcKeeperState.KEEPER_STATE_EXPIRED;
            case Closed:
                return RpcKeeperState.KEEPER_STATE_CLOSED;
        }
    }

    static RpcEventType fromEventType(Watcher.Event.EventType eventType) {
        switch (eventType) {
            default:
                return RpcEventType.EVENT_TYPE_UNKNOWN;
            case None:
                return RpcEventType.EVENT_TYPE_NONE;
            case NodeCreated:
                return RpcEventType.EVENT_TYPE_NODE_CREATED;
            case NodeDeleted:
                return RpcEventType.EVENT_TYPE_NODE_DELETED;
            case NodeDataChanged:
                return RpcEventType.EVENT_TYPE_NODE_DATA_CHANGED;
            case NodeChildrenChanged:
                return RpcEventType.EVENT_TYPE_NODE_CHILDREN_CHANGED;
            case DataWatchRemoved:
                return RpcEventType.EVENT_TYPE_DATA_WATCH_REMOVED;
            case ChildWatchRemoved:
                return RpcEventType.EVENT_TYPE_CHILD_WATCH_REMOVED;
            case PersistentWatchRemoved:
                return RpcEventType.EVENT_TYPE_PERSISTENT_WATCH_REMOVED;
        }
    }

    static RpcWatcherEventResponse fromWatchedEvent(Chroot chroot, WatchedEvent watchedEvent) {
        return RpcWatcherEventResponse.newBuilder()
                .setPath(chroot.unFixPath(watchedEvent.getPath()))
                .setType(fromEventType(watchedEvent.getType()))
                .setState(fromKeeperState(watchedEvent.getState()))
                .build();
    }

    static RpcStat fromStat(Stat stat) {
        return RpcStat.newBuilder()
                .setAversion(stat.getAversion())
                .setCtime(stat.getCtime())
                .setCversion(stat.getCversion())
                .setCzxid(stat.getCzxid())
                .setDataLength(stat.getDataLength())
                .setEphemeralOwner(stat.getEphemeralOwner())
                .setMtime(stat.getMtime())
                .setMzxid(stat.getMzxid())
                .setNumChildren(stat.getNumChildren())
                .setPzxid(stat.getPzxid())
                .setVersion(stat.getVersion())
                .build();
    }

    static RpcId fromId(Id id) {
        return RpcId.newBuilder()
                .setId(id.getId())
                .setScheme(id.getScheme())
                .build();
    }

    static int toWatcherType(RpcWatcherType type) {
        switch (type) {
            default:
            case WATCHER_TYPE_UNKNOWN:
                return 0;
            case WATCHER_TYPE_CHILDREN:
                return Watcher.WatcherType.Children.getIntValue();
            case WATCHER_TYPE_DATA:
                return Watcher.WatcherType.Data.getIntValue();
            case WATCHER_TYPE_ANY:
                return Watcher.WatcherType.Any.getIntValue();
        }
    }

    static int toAddWatchMode(RpcAddWatchMode mode) {
        switch (mode) {
            default:
            case ADD_WATCH_MODE_UNKNOWN:
                return 0;
            case ADD_WATCH_MODE_PERSISTENT:
                return AddWatchMode.PERSISTENT.getMode();
            case ADD_WATCH_MODE_PERSISTENT_RECURSIVE:
                return AddWatchMode.PERSISTENT_RECURSIVE.getMode();
        }
    }

    static List<RpcZooDefsPerms> fromPerms(int perms) {
        List<RpcZooDefsPerms> list = new ArrayList<>();
        if ((perms & ZooDefs.Perms.READ) == ZooDefs.Perms.READ) {
            list.add(RpcZooDefsPerms.PERMS_READ);
        }
        if ((perms & ZooDefs.Perms.WRITE) == ZooDefs.Perms.WRITE) {
            list.add(RpcZooDefsPerms.PERMS_WRITE);
        }
        if ((perms & ZooDefs.Perms.CREATE) == ZooDefs.Perms.CREATE) {
            list.add(RpcZooDefsPerms.PERMS_CREATE);
        }
        if ((perms & ZooDefs.Perms.DELETE) == ZooDefs.Perms.DELETE) {
            list.add(RpcZooDefsPerms.PERMS_DELETE);
        }
        if ((perms & ZooDefs.Perms.ADMIN) == ZooDefs.Perms.ADMIN) {
            list.add(RpcZooDefsPerms.PERMS_ADMIN);
        }
        if ((perms & ZooDefs.Perms.ALL) == ZooDefs.Perms.ALL) {
            list.add(RpcZooDefsPerms.PERMS_ALL);
        }
        return list;
    }

    static RpcACL fromAcl(ACL acl) {
        return RpcACL.newBuilder()
                .setId(fromId(acl.getId()))
                .addAllPerms(fromPerms(acl.getPerms()))
                .build();
    }

    static RpcACLs fromAcls(List<ACL> aclList) {
        return RpcACLs.newBuilder()
                .addAllAcls(aclList.stream().map(Mapper::fromAcl).collect(Collectors.toList()))
                .setPredefinedId(RpcZooDefsIds.IDS_UNKNOWN)   // do we need to return the predefined value that matches?
                .build();
    }

    static RpcConfigDataMultipleAddress fromMultipleAddresses(MultipleAddresses multipleAddresses) {
        return RpcConfigDataMultipleAddress.newBuilder()
                .addAllAddresses(multipleAddresses.getAllAddresses().stream().map(InetSocketAddress::toString).collect(Collectors.toList()))
                .setTimeoutMs(multipleAddresses.getTimeout().toMillis())
                .build();
    }

    static RpcConfigDataServer fromQuorumServer(QuorumPeer.QuorumServer quorumServer) {
        return RpcConfigDataServer.newBuilder()
                .setAddr(fromMultipleAddresses(quorumServer.addr))
                .setElectionAddr(fromMultipleAddresses(quorumServer.electionAddr))
                .setClientAddr(quorumServer.clientAddr.toString())
                .setId(quorumServer.id)
                .setHostname(quorumServer.hostname)
                .setIsParticipant(quorumServer.type == QuorumPeer.LearnerType.PARTICIPANT)
                .setIsClientAddrFromStatic(quorumServer.isClientAddrFromStatic)
                .build();
    }

    static RpcConfigData fromQuorumMaj(QuorumMaj quorumMaj) {
        RpcConfigData.Builder builder = RpcConfigData.newBuilder();
        quorumMaj.getAllMembers().forEach((id, server) -> builder.putAllMembers(id, fromQuorumServer(server)));
        quorumMaj.getObservingMembers().forEach((id, server) -> builder.putObservingMembers(id, fromQuorumServer(server)));
        quorumMaj.getVotingMembers().forEach((id, server) -> builder.putVotingMembers(id, fromQuorumServer(server)));
        return builder
                .setHalf(quorumMaj.getHalf())
                .setVersion(quorumMaj.getVersion())
                .build();
    }

    static Op toOperation(Chroot chroot, RpcTransactionOperation operation) {
        switch (operation.getOperationCase()) {
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation.getOperationCase());
            case CREATE:
                RpcTransactionCreate create = operation.getCreate();
                CreateMode createMode = toCreateMode(create.getCreateMode());
                if (createMode.isTTL()) {
                    return Op.create(chroot.fixPath(create.getPath()), create.getData().toByteArray(), toAcls(create.getACLs()), createMode.toFlag(), create.getTtl());
                }
                return Op.create(chroot.fixPath(create.getPath()), create.getData().toByteArray(), toAcls(create.getACLs()), createMode.toFlag());
            case DELETE:
                RpcTransactionDelete delete = operation.getDelete();
                return Op.delete(chroot.fixPath(delete.getPath()), delete.getVersion());
            case SETDATA:
                RpcTransactionSetData setData = operation.getSetData();
                return Op.setData(chroot.fixPath(setData.getPath()), setData.getData().toByteArray(), setData.getVersion());
            case CHECK:
                RpcTransactionCheck check = operation.getCheck();
                return Op.check(chroot.fixPath(check.getPath()), check.getVersion());
            case GETCHILDREN:
                RpcTransactionGetChildren getChildren = operation.getGetChildren();
                return Op.getChildren(chroot.fixPath(getChildren.getPath()));
            case GETDATA:
                RpcTransactionGetData getData = operation.getGetData();
                return Op.getData(chroot.fixPath(getData.getPath()));
        }
    }

    private Mapper() {
    }
}
