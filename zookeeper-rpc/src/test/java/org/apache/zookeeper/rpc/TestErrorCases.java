package org.apache.zookeeper.rpc;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.zookeeper.rpc.client.AsyncTestClient;
import org.apache.zookeeper.rpc.client.PingService;
import org.apache.zookeeper.rpc.client.SyncTestClient;
import org.apache.zookeeper.rpc.client.ZooKeeperException;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.junit.Test;

public class TestErrorCases extends RpcTestBase {
    @Test(expected = ZooKeeperException.class)
    public void testNoServer() {
        SyncTestClient client = new SyncTestClient(() -> InetSocketAddress.createUnresolved("255.255.255.255", 1234), __ -> {});
        client.connect(1000, null);
    }

    @Test(expected = ZooKeeperException.class)
    public void testMissedClientPing() throws InterruptedException {
        try(SyncTestClient client = newSyncClient()) {
            client.stopPingService();
            Thread.sleep(client.connectionInfo().getSessionTimeOut() * 2);
            client.getChildren("/", false);
        }
    }

    @Test(expected = ZooKeeperException.class)
    public void testMissedServerPing() throws InterruptedException {
        try(SyncTestClient client = new SyncTestClient(this::buildMissedServerPingClient, Duration.ofMillis(SyncTestClient.DEFAULT_RESPONSE_TIMEOUT_MS), __ -> {})) {
            client.start();
            client.connect(SESSION_TIMEOUT, getChrootPath());
            Thread.sleep(client.connectionInfo().getSessionTimeOut() * 2);
            client.getChildren("/", false);
        }
    }

    private AsyncTestClient buildMissedServerPingClient(Consumer<RpcResponse> proc) {
        return new AsyncTestClient(this::serverAddress, proc) {
            @Override
            protected PingService newPingService(int sessionTimeOut) {
                PingService pingService = super.newPingService(sessionTimeOut);
                return new PingService() {
                    @Override
                    public void start() {
                        pingService.start();
                    }

                    @Override
                    public void close() {
                        pingService.close();
                    }

                    @Override
                    public void noteRequestSent() {
                        pingService.noteRequestSent();
                    }

                    @Override
                    public void noteResponseReceived() {
                        // do nothing to simulate no pings from server
                    }
                };
            }
        };
    }
}
