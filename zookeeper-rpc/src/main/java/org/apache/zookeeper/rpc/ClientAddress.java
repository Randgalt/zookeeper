package org.apache.zookeeper.rpc;

import io.grpc.Context;

import java.net.InetSocketAddress;

/**
 * Holder for the client address in context
 */
class ClientAddress {
    private final InetSocketAddress clientAddress;

    static final Context.Key<ClientAddress> KEY = Context.key(ClientAddress.class.getName());

    ClientAddress(InetSocketAddress clientAddress) {
        this.clientAddress = clientAddress;
    }

    InetSocketAddress get() {
        return clientAddress;
    }
}
