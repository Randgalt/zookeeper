package org.apache.zookeeper.server.rpc;

import java.util.List;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Id;

/**
 * The Jute definition for ACL is lacking due to Jute limitations. {@code JuteRpcProcessor}
 * will replace the Jute definition with this.
 */
@SuppressWarnings("unused")
class ACL {
    List<ZooDefs.Perms> perms;
    Id id;
}
