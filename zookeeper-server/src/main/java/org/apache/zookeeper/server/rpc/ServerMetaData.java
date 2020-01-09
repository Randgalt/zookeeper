package org.apache.zookeeper.server.rpc;

import static org.apache.jute.rpc.JuteRpcSpecialHandling.CREATE_EMPTY_REQUEST;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.INLINE_RETURN_TYPE;
import java.util.Map;
import org.apache.jute.rpc.JuteRpc;

@SuppressWarnings("unused")
class ServerMetaData {
    private int version;
    private Map<Integer, String> supportedMessages;
    private int minXid;
    private int maxXid;
    private int minDataVersion;
    private int maxDataVersion;

    @JuteRpc(value = 18, responseName = "metaData", specialHandling = {INLINE_RETURN_TYPE, CREATE_EMPTY_REQUEST})
    native ServerMetaData getServerMetaData();
}
