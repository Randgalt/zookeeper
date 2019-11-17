package org.apache.zookeeper.grpc;

import static org.apache.zookeeper.server.ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

public class ServerTester {
    public static void main(String[] args) {
        System.setProperty(ZOOKEEPER_SERVER_CNXN_FACTORY, RpcServerCnxnFactory.class.getName());
        QuorumPeerMain.main(new String[]{"/Users/jordanzimmerman/dev/oss/apache/zookeeper/conf/zoo_sample.cfg"});
    }
}
