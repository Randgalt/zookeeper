package org.apache.zookeeper.rpc.client;

import org.apache.zookeeper.rpc.generated.v1.RpcKeeperExceptionCode;
import org.apache.zookeeper.rpc.generated.v1.RpcRequest;
import org.apache.zookeeper.rpc.generated.v1.RpcResponse;
import org.apache.zookeeper.rpc.generated.v1.RpcWatcherEventResponse;

import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

class PendingRequestsService {
    private final Deque<Request> requestStack = new ConcurrentLinkedDeque<>();
    private final WatcherService watchService;

    private static class Request {
        private final long xid;
        private final String procValue;
        private final WatcherService.Type type;

        private Request(long xid, String path, WatcherService.Type type) {
            this.xid = xid;
            this.procValue = path;
            this.type = type;
        }
    }

    PendingRequestsService(WatcherService watchService) {
        this.watchService = watchService;
    }

    void noteOutgoingRequest(RpcRequest request, String path, WatcherService.Type type) {
        requestStack.addFirst(new Request(request.getXid(), path, type));
    }

    void clear() {
        requestStack.clear();
    }

    void noteIncomingResponse(RpcResponse response) {
        if (response.getMessageCase() == RpcResponse.MessageCase.WATCHEREVENT) {
            RpcWatcherEventResponse watcherEvent = response.getWatcherEvent();
            watchService.removeWatch(watcherEvent.getType(), watcherEvent.getPath());
        } else {
            try {
                Request poppedRequest = requestStack.removeLast();
                if ((response.getXid() > 0) && (poppedRequest.xid != response.getXid())) {
                    throw new ZooKeeperException(RpcKeeperExceptionCode.CODE_APIERROR); // TODO
/*
            TODO
                packet.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
                throw new IOException("Xid out of order. Got Xid " + replyHdr.getXid()
                        + " with err " + replyHdr.getErr()
                        + " expected Xid " + packet.requestHeader.getXid()
                        + " for a packet with details: " + packet);
*/
                }

                watchService.addWatch(poppedRequest.type, response.getError(), poppedRequest.procValue);
            } catch (NoSuchElementException e) {
                // TODO
            }
        }
    }
}
