package org.apache.zookeeper.server.rpc;

import static org.apache.jute.rpc.JuteRpcSpecialHandling.INLINE_RETURN_TYPE;
import org.apache.jute.rpc.JuteRpc;
import org.apache.zookeeper.Watcher;

@SuppressWarnings("unused")
class WatcherEvent {
    Watcher.Event.EventType type;
    Watcher.Event.KeeperState state;
    String path;

    /**
     * RPC server push a watcher is triggered
     */
    @JuteRpc(value = 10, responseName = "event", specialHandling = INLINE_RETURN_TYPE)
    native WatcherEvent watcherEvent();
}
