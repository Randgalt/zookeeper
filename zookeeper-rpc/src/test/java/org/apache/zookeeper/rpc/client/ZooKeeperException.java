package org.apache.zookeeper.rpc.client;

import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;

public class ZooKeeperException extends RuntimeException {
    private RpcKeeperExceptionCode code;

    public ZooKeeperException(RpcKeeperExceptionCode code) {
        this.code = code;
    }

    public RpcKeeperExceptionCode getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return getCode().name();
    }
}
