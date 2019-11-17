package org.apache.zookeeper.grpc;

import java.net.InetSocketAddress;
import io.grpc.Context;

public class CallContext {
    private final InetSocketAddress clientAddress;

    public static final Context.Key<CallContext> KEY = Context.key(CallContext.class.getName());

    public CallContext(InetSocketAddress clientAddress) {
        this.clientAddress = clientAddress;
    }

    public InetSocketAddress getClientAddress() {
        return clientAddress;
    }
}
