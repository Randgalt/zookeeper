package org.apache.zookeeper.rpc;

import org.apache.zookeeper.rpc.generated.v1.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

class RpcResponseBuilder {
    private static final Map<Class<?>, BiFunction<RpcResponse.Builder, ?, RpcResponse.Builder>> map;

    private static <T> void add(Map<Class<?>, BiFunction<RpcResponse.Builder, ?, RpcResponse.Builder>> map,
                                Class<T> clazz,
                                BiFunction<RpcResponse.Builder, T, RpcResponse.Builder> proc) {
        map.put(clazz, proc);
    }

    static {
        Map<Class<?>, BiFunction<RpcResponse.Builder, ?, RpcResponse.Builder>> workMap = new HashMap<>();
        add(workMap, RpcConnectResponse.class, RpcResponse.Builder::setConnect);
        add(workMap, RpcCreateResponse.class, RpcResponse.Builder::setCreate);
        add(workMap, RpcGetDataResponse.class, RpcResponse.Builder::setGetData);
        add(workMap, RpcSetDataResponse.class, RpcResponse.Builder::setSetData);
        add(workMap, RpcWatcherEventResponse.class, RpcResponse.Builder::setWatcherEvent);
        add(workMap, RpcFourLetterResponse.class, RpcResponse.Builder::setFourLetter);
        add(workMap, RpcFourLetterSetTraceMaskResponse.class, RpcResponse.Builder::setFourLetterSetTraceMask);
        add(workMap, RpcGetChildrenResponse.class, RpcResponse.Builder::setGetChildren);
        add(workMap, RpcGetAllChildrenNumberResponse.class, RpcResponse.Builder::setGetAllChildrenNumber);
        add(workMap, RpcGetEphemeralsResponse.class, RpcResponse.Builder::setGetEphemerals);
        add(workMap, RpcGetEphemeralsResponse.class, RpcResponse.Builder::setGetEphemerals);
        add(workMap, RpcTransactionResponse.class, RpcResponse.Builder::setTransaction);
        add(workMap, RpcGetConfigResponse.class, RpcResponse.Builder::setGetConfig);
        add(workMap, RpcGetServerMetaDataResponse.class, RpcResponse.Builder::setGetServerMetaData);
        add(workMap, RpcExistsResponse.class, RpcResponse.Builder::setExists);
        map = Collections.unmodifiableMap(workMap);
    }

    static <T> RpcResponse build(long xid, long zxid, RpcKeeperExceptionCode code, T response) {
        @SuppressWarnings("unchecked")  // is safe due to how map is built
        BiFunction<RpcResponse.Builder, T, RpcResponse.Builder> proc = (BiFunction<RpcResponse.Builder, T, RpcResponse.Builder>) map.get(response.getClass());
        if (proc == null) {
            throw new IllegalArgumentException("No mapping for: " + response.getClass());
        }

        RpcResponse.Builder builder = builder(xid, zxid, code);
        if (code == RpcKeeperExceptionCode.CODE_OK) {
            proc.apply(builder, response);
        }
        return builder.build();
    }

    static <T> RpcResponse build(long xid, long zxid, RpcKeeperExceptionCode code) {
        return builder(xid, zxid, code).build();
    }

    private static RpcResponse.Builder builder(long xid, long zxid, RpcKeeperExceptionCode code) {
        return RpcResponse.newBuilder()
                    .setXid(xid)
                    .setZxid(zxid)
                    .setError(code);
    }

    private RpcResponseBuilder() {
    }
}
