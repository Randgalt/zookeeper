package org.apache.zookeeper.grpc;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.grpc.client.Xid;
import org.apache.zookeeper.grpc.generated.RpcAuthInfo;
import org.apache.zookeeper.grpc.generated.RpcConnectRequest;
import org.apache.zookeeper.grpc.generated.RpcCreateRequest;
import org.apache.zookeeper.grpc.generated.RpcDeleteRequest;
import org.apache.zookeeper.grpc.generated.RpcGetChildrenRequest;
import org.apache.zookeeper.grpc.generated.RpcGetDataRequest;
import org.apache.zookeeper.grpc.generated.RpcRequest;
import org.apache.zookeeper.grpc.generated.RpcRequestHeader;
import org.apache.zookeeper.grpc.generated.RpcResponse;
import org.apache.zookeeper.grpc.generated.RpcSetDataRequest;
import org.apache.zookeeper.grpc.generated.ZooKeeperServiceGrpc;

public class ClientTester {
    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 2181).usePlaintext().build();
        ZooKeeperServiceGrpc.ZooKeeperServiceStub service = ZooKeeperServiceGrpc.newStub(channel);
        CountDownLatch readyLatch = new CountDownLatch(1);

        AtomicLong lastSeenZxid = new AtomicLong(0);
        StreamObserver<RpcResponse> responses = new StreamObserver<RpcResponse>() {
            @Override
            public void onNext(RpcResponse response) {
                if (response.hasHeader()) {
                    lastSeenZxid.set(response.getHeader().getZxid());
                }
                if (response.hasConnect()) {
                    readyLatch.countDown();
                }
                System.out.println(response);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
        StreamObserver<RpcRequest> requests = service.process(responses);

        Xid xid = new Xid();

        RpcConnectRequest connectRequest = RpcConnectRequest.newBuilder().setTimeOut(100000).build();
        requests.onNext(rpc(xid).setConnect(connectRequest).build());

        readyLatch.await();

        RpcAuthInfo authInfo = RpcAuthInfo.newBuilder()
                .setScheme("digest")
                .setAuth(ByteString.copyFrom("me1:pass1", Charset.defaultCharset()))
                .build();
        requests.onNext(rpc(xid).setAuth(authInfo).build());

        RpcCreateRequest createRequest = RpcCreateRequest.newBuilder()
                .addAllAcl(RpcMappers.fromAcls(ZooDefs.Ids.OPEN_ACL_UNSAFE))
                .setPath("/test")
                .setData(ByteString.copyFrom("hello", Charset.defaultCharset()))
                .build();
        requests.onNext(rpc(xid).setCreate(createRequest).build());

        RpcGetDataRequest getDataRequest = RpcGetDataRequest.newBuilder()
                .setPath("/test")
                .setWatch(true)
                .build();
        requests.onNext(rpc(xid).setGetData(getDataRequest).build());

        RpcSetDataRequest setDataRequest = RpcSetDataRequest.newBuilder()
                .setPath("/test")
                .setData(ByteString.copyFrom("yo", Charset.defaultCharset()))
                .setVersion(-1)
                .build();
        requests.onNext(rpc(xid).setSetData(setDataRequest).build());

        RpcGetChildrenRequest getChildrenRequest = RpcGetChildrenRequest.newBuilder()
                .setPath("/")
                .setWatch(true)
                .build();
        requests.onNext(rpc(xid).setGetChildren(getChildrenRequest).build());

        RpcDeleteRequest deleteRequest = RpcDeleteRequest.newBuilder()
                .setPath("/test")
                .setVersion(-1)
                .build();
        requests.onNext(rpc(xid).setDelete(deleteRequest).build());

        Thread.currentThread().join();
    }

    private static RpcRequest.Builder rpc(Xid xid) {
        return RpcRequest.newBuilder().setHeader(RpcRequestHeader.newBuilder().setXid(xid.getXid()).build());
    }
}
