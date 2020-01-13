package org.apache.zookeeper.rpc.client;

import com.google.protobuf.ByteString;
import org.apache.zookeeper.rpc.generated.v1.*;

public class TransactionBuilder {
    private final RpcTransactionRequest.Builder builder = RpcTransactionRequest.newBuilder();

    public static TransactionBuilder builder() {
        return new TransactionBuilder();
    }

    public RpcTransactionRequest build() {
        return builder.build();
    }

    public TransactionBuilder create(String path, byte[] data, RpcCreateMode createMode, long ttl) {
        RpcACLs acls = RpcACLs.newBuilder().setPredefinedId(RpcZooDefsIds.IDS_OPEN_ACL_UNSAFE).build();
        RpcTransactionCreate create = RpcTransactionCreate.newBuilder()
                .setPath(path)
                .setData(ByteString.copyFrom(data))
                .setACLs(acls)
                .setCreateMode(createMode)
                .setTtl(ttl)
                .build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setCreate(create).build());
        return this;
    }

    public TransactionBuilder delete(String path, int version) {
        RpcTransactionDelete delete = RpcTransactionDelete.newBuilder().setPath(path).setVersion(version).build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setDelete(delete).build());
        return this;
    }

    public TransactionBuilder setData(String path, byte[] data, int version) {
        RpcTransactionSetData setData = RpcTransactionSetData.newBuilder()
                .setPath(path)
                .setData(ByteString.copyFrom(data))
                .setVersion(version)
                .build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setSetData(setData).build());
        return this;
    }

    public TransactionBuilder check(String path, int version) {
        RpcTransactionCheck check = RpcTransactionCheck.newBuilder().setPath(path).setVersion(version).build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setCheck(check).build());
        return this;
    }

    public TransactionBuilder getChildren(String path) {
        RpcTransactionGetChildren getChildren = RpcTransactionGetChildren.newBuilder().setPath(path).build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setGetChildren(getChildren).build());
        return this;
    }

    public TransactionBuilder getData(String path) {
        RpcTransactionGetData getData = RpcTransactionGetData.newBuilder().setPath(path).build();
        builder.addOperations(RpcTransactionOperation.newBuilder().setGetData(getData).build());
        return this;
    }

    private TransactionBuilder() {
    }
}
