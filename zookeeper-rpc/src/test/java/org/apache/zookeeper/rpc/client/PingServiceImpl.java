package org.apache.zookeeper.rpc.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class PingServiceImpl implements PingService {
    private final TestClient client;
    private final long pingThresholdMs;
    private final long disconnectThresholdMs;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private volatile long lastRequestSentNanos = 0;
    private volatile long lastResponseReceivedNanos = 0;

    PingServiceImpl(TestClient client, long sessionTimeout) {
        this.client = client;
        pingThresholdMs = (sessionTimeout / 2);
        disconnectThresholdMs = 2 * (sessionTimeout / 3);
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            long initialDelay = Math.max(1, pingThresholdMs / 2);
            executorService.scheduleAtFixedRate(this::checkPing, initialDelay, initialDelay, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            executorService.shutdownNow();
        }
    }

    @Override
    public void noteRequestSent() {
        lastRequestSentNanos = System.nanoTime();
    }

    @Override
    public void noteResponseReceived() {
        lastResponseReceivedNanos = System.nanoTime();
    }

    private void checkPing() {
        long msSinceLastPingReceived = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastResponseReceivedNanos);
        if (msSinceLastPingReceived >= disconnectThresholdMs) {
            client.stop();  // TODO
        } else {
            long msSinceLastPingSent = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRequestSentNanos);
            if (msSinceLastPingSent >= pingThresholdMs) {
                client.ping();
            }
        }
    }
}
