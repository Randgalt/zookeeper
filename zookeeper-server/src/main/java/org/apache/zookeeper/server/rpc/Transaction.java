package org.apache.zookeeper.server.rpc;

import java.util.List;
import org.apache.jute.rpc.JuteRpc;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Using the {@link JuteRpc} annotation on the transaction methods cannot work. It would be nice to come up with a
 * way to make the transaction classes/methods annotation-enabled so that this workaround isn't necessary. For now,
 * these are gRPC-specific classes for handling transactions. The server-side code will need to marshall between
 * these and the Jute records used by the server.
 */
@SuppressWarnings("unused")
class Transaction {
    static class Create {
        String path;
        byte[] data;
        List<ACL> acl;
        CreateMode createMode;
        long ttl;
    }

    static class CreateResult {
        String path;
        Stat stat;
    }

    static class Delete {
        String path;
        int version;
    }

    static class DeleteResult {
    }

    static class SetData {
        String path;
        byte[] data;
        int version;
    }

    static class SetDataResult {
        Stat stat;
    }

    static class Check {
        String path;
        int version;
    }

    static class CheckResult {
    }

    static class GetChildren {
        String path;
    }

    static class GetChildrenResult {
        List<String> children;
    }

    static class GetData {
        String path;
    }

    static class GetDataResult {
        byte[] data;
        Stat stat;
    }

    static class ErrorResult {
        KeeperException.Code error;
    }

    // the JuteRpcProcessor handles this specially - makes it a oneof
    static class Operation {
        Create create;
        Delete delete;
        SetData setData;
        Check check;
        GetChildren getChildren;
        GetData getData;
    }

    // the JuteRpcProcessor handles this specially - makes it a {@code oneof}
    static class Result {
        CreateResult create;
        DeleteResult delete;
        SetDataResult setData;
        CheckResult check;
        GetChildrenResult getChildren;
        GetDataResult getData;
        ErrorResult error;
    }

    @JuteRpc(14)
    native List<Result> transaction(List<Operation> operations);
}
