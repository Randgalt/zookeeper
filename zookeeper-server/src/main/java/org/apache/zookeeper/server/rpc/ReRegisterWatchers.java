package org.apache.zookeeper.server.rpc;

import java.util.Set;
import org.apache.jute.rpc.JuteRpc;

@SuppressWarnings("unused")
class ReRegisterWatchers {
    @JuteRpc(19)
    native void reRegisterWatchers(long lastZxid,
                                   Set<String> dataWatches,
                                   Set<String> existWatches,
                                   Set<String> childWatches,
                                   Set<String> persistentWatches, Set<String> persistentRecursiveWatches);
}
