package org.apache.zookeeper.server.rpc;

import static org.apache.jute.rpc.JuteRpcSpecialHandling.CREATE_EMPTY_REQUEST;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.INLINE_RETURN_TYPE;
import org.apache.jute.rpc.JuteRpc;

@SuppressWarnings("unused")
public class Connect {
    private int sessionTimeOut;
    private long sessionId;
    private byte[] password;
    private boolean readOnly;

    @JuteRpc(value = 12, specialHandling = INLINE_RETURN_TYPE)
    native Connect connect(long lastZxidSeen, int sessionTimeOut, long sessionId, byte[] password, boolean canBeReadOnly, String chrootPath);

    @JuteRpc(value = 13, specialHandling = CREATE_EMPTY_REQUEST)
    native void closeSession();
}
