package org.apache.jute.rpc;

/**
 * Various constants used by the RPC processor.
 */
class Constants {
    static final String REQUEST = "Request";
    static final String RESPONSE = "Response";
    static final String ONE_OF_NAME = "message";
    static final String ZOOKEEPER_SERVICE_NAME = "ZooKeeperService";
    static final String STAT = "stat";
    static final String INDENT = "    ";
    static final String PROTO_FILE_NAME = "zookeeper.proto";
    static final String UNKNOWN = "UNKNOWN";
    static final String MAIN_SERVICE_METHOD_NAME = "process";
    static final String RPC_PREFIX = "Rpc";
    static final String SPECIAL_XIDS = "SpecialXids";
    static final String SPECIAL_XID_PREFIX = "SpecialXid";

    static final int MIN_FIELD_NUMBER = 10;

    static final String STAT_CLASS_NAME = "org.apache.zookeeper.data.Stat";
    static final String IDS_CLASS_NAME = "org.apache.zookeeper.ZooDefs.Ids";
    static final String PERMS_CLASS_NAME = "org.apache.zookeeper.ZooDefs.Perms";
    static final String RPC_ACL_LIST_CLASS_NAME = "org.apache.zookeeper.server.rpc.ACLs";
    static final String ACL_LIST_CLASS_NAME = "java.util.List<org.apache.zookeeper.data.ACL>";
    static final String SUBSTITUTE_CONFIG_DATA_CLASS_NAME = "org.apache.zookeeper.server.rpc.ConfigData";
    static final String TRANSACTION_OPERATION_CLASS_NAME = "org.apache.zookeeper.server.rpc.Transaction.Operation";
    static final String TRANSACTION_RESULT_CLASS_NAME = "org.apache.zookeeper.server.rpc.Transaction.Result";
    static final String KEEPER_EXCEPTION_CODE_CLASS_NAME = "org.apache.zookeeper.KeeperException.Code";

    static final String PROTO_FILE_HEADER = "syntax = \"proto3\";\n"
            + "\n"
            + "option java_multiple_files = true;\n"
            + "\n"
            + "package org.apache.zookeeper.rpc.generated.v1;\n"
            + "\n";
}
