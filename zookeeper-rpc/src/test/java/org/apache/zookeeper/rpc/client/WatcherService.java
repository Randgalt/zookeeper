package org.apache.zookeeper.rpc.client;

import org.apache.zookeeper.rpc.generated.v1.RpcEventType;
import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class WatcherService {
    private final Set<String> dataWatches = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> existWatches = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> childWatches = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> persistentWatches = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> persistentRecursiveWatches = Collections.newSetFromMap(new ConcurrentHashMap<>());

    enum Type {
        NONE,
        DATA,
        EXISTS,
        CHILD,
        PERSISTENT,
        PERSISTENT_RECURSIVE;
    }

    void removeWatch(RpcEventType type, String path) {
        switch (type) {
            default:
                // nop
                break;

            case EVENT_TYPE_NODE_CREATED:
            case EVENT_TYPE_NODE_DATA_CHANGED:
                dataWatches.remove(path);
                existWatches.remove(path);
                break;

            case EVENT_TYPE_NODE_DELETED:
                childWatches.remove(path);
                break;

            case EVENT_TYPE_NODE_CHILDREN_CHANGED:
                dataWatches.remove(path);
                existWatches.remove(path);
                childWatches.remove(path);
                break;
        }
    }

    void addWatch(Type type, RpcKeeperExceptionCode code, String path) {
        switch (type) {
            case DATA:
                applyOnOk(code, path, dataWatches);
                break;

            case EXISTS:
                if (code == RpcKeeperExceptionCode.CODE_OK) {
                    dataWatches.add(path);
                } else if (code == RpcKeeperExceptionCode.CODE_NONODE) {
                    existWatches.add(path);
                }
                break;

            case CHILD:
                applyOnOk(code, path, childWatches);
                break;

            case PERSISTENT:
                applyOnOkOrNoNode(code, path, persistentWatches);
                break;

            case PERSISTENT_RECURSIVE:
                applyOnOkOrNoNode(code, path, persistentRecursiveWatches);
                break;

            default:
                // ignore
                break;
        }
    }

    private void applyOnOk(RpcKeeperExceptionCode code, String path, Set<String> watches) {
        if (code == RpcKeeperExceptionCode.CODE_OK) {
            watches.add(path);
        }
    }

    private void applyOnOkOrNoNode(RpcKeeperExceptionCode code, String path, Set<String> watches) {
        if ((code == RpcKeeperExceptionCode.CODE_OK) || (code == RpcKeeperExceptionCode.CODE_NONODE)) {
            watches.add(path);
        }
    }
}
