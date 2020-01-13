package org.apache.zookeeper.rpc;

import com.google.protobuf.ByteString;
import org.apache.zookeeper.rpc.client.SyncTestClient;
import org.apache.zookeeper.rpc.client.TransactionBuilder;
import org.apache.zookeeper.rpc.client.ZooKeeperException;
import org.apache.zookeeper.rpc.generated.v1.RpcCreateMode;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcTransactionResult;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherEventResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.zookeeper.rpc.TestBasicApis.toSet;
import static org.junit.Assert.*;

public class TestChroot extends RpcTestBase {
    private static final String CHROOT = "/foobar";

    private volatile SyncTestClient nonChrootClient;
    private volatile String testChroot = null;

    @Before
    public void setupChroot() {
        nonChrootClient = newSyncClient();
        try {
            nonChrootClient.create(CHROOT, RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
        } catch (ZooKeeperException ignore) {
            // ignore
        }
        testChroot = CHROOT;
    }

    @After
    public void cleanUp() {
        nonChrootClient.close();
        nonChrootClient = null;
        testChroot = null;
    }

    @Test
    public void testCreate() {
        try(SyncTestClient client = newSyncClient()) {
            assertEquals("/tc", client.create("/tc", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.copyFromUtf8("test"), 0).getPath());
            validatePath("/tc");
        }
    }

    @Test
    public void testCreateChildren() {
        try(SyncTestClient client = newSyncClient()) {
            assertEquals("/tcc", client.create("/tcc", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.copyFromUtf8("test"), 0).getPath());
            validatePath("/tcc");
            assertEquals("/tcc/a", client.create("/tcc/a", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.copyFromUtf8("test"), 0).getPath());
            validatePath("/tcc/a");
            assertEquals("/tcc/b", client.create("/tcc/b", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.copyFromUtf8("test"), 0).getPath());
            validatePath("/tcc/b");

            Set<String> expected = toSet(Arrays.asList("a", "b"));
            Set<String> actual = toSet(client.getChildren("/tcc", false).getChildrenList());
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testWatcher() {
        AtomicReference<String> path = new AtomicReference<>();
        Consumer<RpcWatcherEventResponse> watcher = e -> path.set(e.getPath());
        try(SyncTestClient client = newSyncClient(watcher)) {
            client.exists("/tw", true);
            client.create("/tw", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            assertEquals("/tw", path.get());
        }
    }

    @Test
    public void testDelete() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/td", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.delete("/td", -1);
            assertFalse(nonChrootClient.exists(CHROOT + "/td", false).hasResult());
            assertFalse(nonChrootClient.exists("/td", false).hasResult());
        }
    }

    @Test
    public void testTransaction() {
        try(SyncTestClient client = newSyncClient()) {
            client.create("/tt", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            client.create("/tt2", RpcCreateMode.CREATE_MODE_PERSISTENT, ByteString.EMPTY, 0);
            RpcTransactionRequest transaction = TransactionBuilder.builder()
                    .create("/tt/one", "what".getBytes(), RpcCreateMode.CREATE_MODE_PERSISTENT, 0)
                    .delete("/tt2", -1)
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
            validatePath("/tt");
            validatePath("/tt/one");

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

    // TODO - remaining APIs

    private void validatePath(String path) {
        assertTrue(nonChrootClient.exists(CHROOT + path, false).hasResult());
        assertFalse(nonChrootClient.exists(path, false).hasResult());
    }

    @Override
    protected String getChrootPath() {
        return testChroot;
    }
}
