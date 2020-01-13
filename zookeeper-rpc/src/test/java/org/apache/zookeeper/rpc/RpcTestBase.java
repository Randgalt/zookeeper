/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.rpc;

import org.apache.zookeeper.rpc.client.SyncTestClient;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherEventResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * Run tests with: Netty Client against Netty server
 */
public abstract class RpcTestBase extends ZKTestCase {
    protected static final int CONTAINER_CHECK_INTERVAL_MS = 500;
    protected static final int SESSION_TIMEOUT = 1000;

    private static final int SERVER_PORT = PortAssignment.unique();

    private static ServerCnxnFactory serverCnxnFactory;
    private static ZooKeeperServer zooKeeperServer;
    private static ContainerManager containerManager;

    @BeforeClass
    public static void setUpRpc() throws IOException, InterruptedException {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, RpcServerConnectionFactory.class.getName());
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
        System.setProperty("zookeeper.admin.enableServer", "true");

        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        zooKeeperServer = new ZooKeeperServer(tmpDir, tmpDir, 1000);
        SyncRequestProcessor.setSnapCount(1000);
        serverCnxnFactory = ServerCnxnFactory.createFactory(SERVER_PORT, -1);
        serverCnxnFactory.startup(zooKeeperServer);

        containerManager = new ContainerManager(zooKeeperServer.getZKDatabase(), zooKeeperServer.getFirstProcessor(), CONTAINER_CHECK_INTERVAL_MS, 10000, 0);
        containerManager.start();
    }

    @AfterClass
    public static void tearDownRpc() {
        containerManager.stop();
        serverCnxnFactory.shutdown();
        zooKeeperServer.shutdown();

        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
        System.clearProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
    }

    protected SyncTestClient newSyncClient() {
        return newSyncClient(SESSION_TIMEOUT, __ -> {});
    }

    protected SyncTestClient newSyncClient(Consumer<RpcWatcherEventResponse> watcherHandler) {
        return newSyncClient(SESSION_TIMEOUT, watcherHandler);
    }

    protected SyncTestClient newSyncClient(int sessionTimeout, Consumer<RpcWatcherEventResponse> watcherHandler) {
        InetSocketAddress address = serverAddress();
        SyncTestClient client = new SyncTestClient(() -> address, watcherHandler);
        client.start();
        client.connect(sessionTimeout, getChrootPath());
        return client;
    }

    protected InetSocketAddress serverAddress() {
        return InetSocketAddress.createUnresolved("localhost", SERVER_PORT);
    }

    protected String getChrootPath() {
        return null;
    }
}
