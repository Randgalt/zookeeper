package org.apache.zookeeper.rpc;

import com.google.protobuf.ByteString;
import org.apache.zookeeper.rpc.client.SyncTestClient;
import org.apache.zookeeper.rpc.client.TestClient;
import org.apache.zookeeper.rpc.client.TransactionBuilder;
import org.apache.zookeeper.rpc.generated.v1.*;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.zookeeper.rpc.generated.v1.RpcEventType.EVENT_TYPE_NODE_CREATED;
import static org.junit.Assert.*;

public class TestBasicApis extends RpcTestBase {
    @Test
    public void testPing() {
        try(TestClient client = newSyncClient()) {
            client.ping();
        }
    }

    @Test
    public void testGetServerMetaData() {
        try(SyncTestClient client = newSyncClient()) {
            assertEquals(1, client.getServerMetaData().getVersion());
        }
    }

    @Test
    public void testGetConfig() {
        try(SyncTestClient client = newSyncClient()) {
            client.getConfig(false);
        }
    }

    @Test
    public void testExistsFalseWithWatcherThenCreateNode() {
        AtomicInteger counter = new AtomicInteger();
        Consumer<RpcWatcherEventResponse> watcher = event -> {
            if (event.getType() == EVENT_TYPE_NODE_CREATED) {
                counter.incrementAndGet();
            }
        };
        try(SyncTestClient client = newSyncClient(watcher)) {
            assertFalse(client.exists("/tef", true).hasResult());

            assertEquals("/tef", client.create("/tef", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0).getPath());
            assertEquals(counter.get(), 1);
        }
    }

    @Test
    public void testReRegisterWatcher() {
        AtomicInteger counter = new AtomicInteger();
        Consumer<RpcWatcherEventResponse> watcher = event -> {
            if (event.getType() == EVENT_TYPE_NODE_CREATED) {
                counter.incrementAndGet();
            }
        };
        try(SyncTestClient client = newSyncClient(watcher)) {
            client.reRegisterWatchers(emptySet(), singleton("/rere"), emptySet(), emptySet(), emptySet());

            assertEquals("/rere", client.create("/rere", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0).getPath());
            assertEquals(counter.get(), 1);
        }
    }

    @Test
    public void testReconnect() throws InterruptedException {
        try(SyncTestClient client = newSyncClient(10000, __ -> {})) {   // large session timeout so that it stays active
            client.create("/reconTest", RpcCreateMode.CREATE_MODE_EPHEMERAL, ByteString.EMPTY, 0);

            client.stop();
            assertTrue(client.awaitTermination(1000));
            client.start();
            client.reconnect();

            assertTrue(client.exists("/reconTest", false).hasResult());
        }
    }

    @Test
    public void testGetChildren() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/tgc", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.create("/tgc/a", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.create("/tgc/b", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            assertEquals(toSet(asList("a", "b")), toSet(client.getChildren("/tgc", false).getChildrenList()));
            assertEquals(2, client.getAllChildrenNumber("/tgc").getTotalNumber());
        }
    }

    @Test
    public void testGetSetData() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/gsd", RpcCreateMode.CREATE_MODE_PERSISTENT, copyFromUtf8("first"), 0);
            RpcGetDataResponse response = client.getData("/gsd", false);
            assertEquals("first", response.getData().toStringUtf8());
            client.setData("/gsd", copyFromUtf8("new value"), response.getStat().getVersion());
            response = client.getData("/gsd", false);
            assertEquals("new value", response.getData().toStringUtf8());
        }
    }

    @Test
    public void testEphemeralNode() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/ten", RpcCreateMode.CREATE_MODE_EPHEMERAL, ByteString.EMPTY, 0);
            assertTrue(client.exists("/ten", true).hasResult());
        }
        try(SyncTestClient client = newSyncClient()) {
            assertFalse(client.exists("/ten", false).hasResult());
        }
    }

    @Test
    public void testTtlNode() throws InterruptedException {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/tttl", RpcCreateMode.CREATE_MODE_PERSISTENT_WITH_TTL, ByteString.EMPTY, CONTAINER_CHECK_INTERVAL_MS * 2);
            assertTrue(client.exists("/tttl", true).hasResult());
            Thread.sleep(CONTAINER_CHECK_INTERVAL_MS * 3);
            assertFalse(client.exists("/tttl", true).hasResult());
        }
    }

    @Test
    public void testRemoveWatches() throws InterruptedException {
        Semaphore semaphore = new Semaphore(0);
        Consumer<RpcWatcherEventResponse> watcher = __ -> semaphore.release();
        try(SyncTestClient client = newSyncClient(watcher)) {
            client.exists("/tttl", true);
            client.removeAllWatches("/tttl", RpcWatcherType.WATCHER_TYPE_ANY, false);
            client.create("/tttl", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            assertFalse(semaphore.tryAcquire(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testAddWatch() throws InterruptedException {
        Semaphore semaphore = new Semaphore(0);
        Consumer<RpcWatcherEventResponse> watcher = __ -> semaphore.release();
        try(SyncTestClient client = newSyncClient(watcher)) {
            client.addWatch("/taw", RpcAddWatchMode.ADD_WATCH_MODE_PERSISTENT_RECURSIVE);
            client.create("/taw", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.create("/taw/one", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.setData("/taw/one", ByteString.copyFromUtf8("hey"), -1);
            client.create("/taw/two", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            assertTrue(semaphore.tryAcquire(4, SyncTestClient.DEFAULT_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testTransaction() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/tt", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            RpcTransactionRequest transaction = TransactionBuilder.builder()
                    .create("/tt/one", "what".getBytes(), RpcCreateMode.CREATE_MODE_PERSISTENT, 0)
                    .delete("/tt/one", -1)
                    .check("/tt", -1)
                    .setData("/tt", "new".getBytes(), -1)
                    .build();
            List<RpcTransactionResult.ResultCase> resultTypes = client.transaction(transaction).getResultList().stream()
                    .map(RpcTransactionResult::getResultCase)
                    .collect(Collectors.toList());
            List<RpcTransactionResult.ResultCase> expected = asList(RpcTransactionResult.ResultCase.CREATE,
                    RpcTransactionResult.ResultCase.DELETE,
                    RpcTransactionResult.ResultCase.CHECK,
                    RpcTransactionResult.ResultCase.SETDATA);
            assertEquals(expected, resultTypes);

            transaction = TransactionBuilder.builder()
                    .getChildren("/tt")
                    .getData("/tt")
                    .build();
            resultTypes = client.transaction(transaction).getResultList().stream()
                    .map(RpcTransactionResult::getResultCase)
                    .collect(Collectors.toList());
            expected = asList(RpcTransactionResult.ResultCase.GETCHILDREN, RpcTransactionResult.ResultCase.GETDATA);
            assertEquals(expected, resultTypes);
        }
    }

    static <T> Set<T> toSet(List<T> l) {
        return new HashSet<>(l);
    }
}
