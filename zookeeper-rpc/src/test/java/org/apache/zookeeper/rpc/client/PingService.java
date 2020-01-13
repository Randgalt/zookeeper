package org.apache.zookeeper.rpc.client;

import java.io.Closeable;

public interface PingService extends Closeable {
    void start();

    @Override
    void close();

    void noteRequestSent();

    void noteResponseReceived();
}
