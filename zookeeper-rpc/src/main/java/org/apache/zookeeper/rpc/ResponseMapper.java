package org.apache.zookeeper.rpc;

import com.google.protobuf.ByteString;
import org.apache.jute.Record;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.rpc.generated.v1.*;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.zookeeper.rpc.Mapper.*;

class ResponseMapper {
    private static final Map<Class<?>, BiFunction<Chroot, ?, ?>> map;
    private static final Map<Class<?>, BiFunction<Chroot, ?, RpcTransactionResult>> opResultMap;

    private static <T extends Record, R> void addToMap(Map<Class<?>, BiFunction<Chroot, ?, ?>> map,
                                                       Class<T> clazz,
                                                       BiFunction<Chroot, T, R> proc) {
        map.put(clazz, proc);
    }

    private static <T extends OpResult> void addToOpResultMap(Map<Class<?>, BiFunction<Chroot, ?, RpcTransactionResult>> map,
                                                       Class<T> clazz,
                                                       BiFunction<Chroot, T, RpcTransactionResult> proc) {
        map.put(clazz, proc);
    }

    static {
        Map<Class<?>, BiFunction<Chroot, ?, ?>> work = new HashMap<>();
        addToMap(work, Create2Response.class, ResponseMapper::toCreateResponse);
        addToMap(work, ExistsResponse.class, ResponseMapper::toExistsResponse);
        addToMap(work, GetDataResponse.class, ResponseMapper::toGetDataResponse);
        addToMap(work, SetDataResponse.class, ResponseMapper::toSetDataResponse);
        addToMap(work, GetACLResponse.class, ResponseMapper::toGetACLResponse);
        addToMap(work, SetACLResponse.class, ResponseMapper::toSetACLResponse);
        addToMap(work, GetChildren2Response.class, ResponseMapper::toGetChildrenResponse);
        addToMap(work, GetAllChildrenNumberResponse.class, ResponseMapper::toGetAllChildrenNumberResponse);
        addToMap(work, GetEphemeralsResponse.class, ResponseMapper::toGetEphemeralsResponse);
        addToMap(work, MultiResponse.class, ResponseMapper::toTransactionResponse);
        map = Collections.unmodifiableMap(work);
    }

    static {
        Map<Class<?>, BiFunction<Chroot, ?, RpcTransactionResult>> work = new HashMap<>();
        addToOpResultMap(work, OpResult.CreateResult.class, ResponseMapper::toCreateResult);
        addToOpResultMap(work, OpResult.DeleteResult.class, ResponseMapper::toDeleteResult);
        addToOpResultMap(work, OpResult.SetDataResult.class, ResponseMapper::toSetDataResult);
        addToOpResultMap(work, OpResult.CheckResult.class, ResponseMapper::toCheckResult);
        addToOpResultMap(work, OpResult.GetChildrenResult.class, ResponseMapper::toGetChildrenResult);
        addToOpResultMap(work, OpResult.GetDataResult.class, ResponseMapper::toGetDataResult);
        addToOpResultMap(work, OpResult.ErrorResult.class, ResponseMapper::toErrorResult);
        opResultMap = Collections.unmodifiableMap(work);
    }

    static RpcGetConfigResponse toConfigData(GetDataResponse record) {
        try {
            Properties properties = new Properties();
            properties.load(new ByteArrayInputStream(record.getData()));
            QuorumMaj quorumMaj = new QuorumMaj(properties);
            return RpcGetConfigResponse.newBuilder()
                    .setStat(fromStat(record.getStat()))
                    .setConfig(fromQuorumMaj(quorumMaj))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse QuorumMaj: " + record);
        }
    }

    // raw usage safe due to how map is built
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T extends Record> Object toResponse(Chroot chroot, T record) {
        BiFunction function = map.get(record.getClass());
        if (function == null) {
            throw new IllegalArgumentException("No mapper found for: " + record.getClass());
        }
        return function.apply(chroot, record);
    }

    // raw usage/casting safe due to how map is built
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T extends OpResult> RpcTransactionResult toOpResult(Chroot chroot, T record) {
        BiFunction function = opResultMap.get(record.getClass());
        return (RpcTransactionResult)function.apply(chroot, record);
    }

    private static RpcCreateResponse toCreateResponse(Chroot chroot, Create2Response record) {
        return RpcCreateResponse.newBuilder()
                .setStat(fromStat(record.getStat()))
                .setPath(chroot.unFixPath(record.getPath()))
                .build();
    }

    private static RpcSetDataResponse toSetDataResponse(Chroot chroot, SetDataResponse record) {
        return RpcSetDataResponse.newBuilder()
                .setResult(fromStat(record.getStat()))
                .build();
    }

    private static RpcExistsResponse toExistsResponse(Chroot chroot, ExistsResponse record) {
        return RpcExistsResponse.newBuilder()
                .setResult(fromStat(record.getStat()))
                .build();
    }

    private static RpcGetDataResponse toGetDataResponse(Chroot chroot, GetDataResponse record) {
        return RpcGetDataResponse.newBuilder()
                .setData(ByteString.copyFrom(record.getData()))
                .setStat(fromStat(record.getStat()))
                .build();
    }

    private static RpcGetChildrenResponse toGetChildrenResponse(Chroot chroot, GetChildren2Response record) {
        return RpcGetChildrenResponse.newBuilder()
                .setStat(fromStat(record.getStat()))
                .addAllChildren(record.getChildren())
                .build();
    }

    private static RpcGetAllChildrenNumberResponse toGetAllChildrenNumberResponse(Chroot chroot, GetAllChildrenNumberResponse record) {
        return RpcGetAllChildrenNumberResponse.newBuilder()
                .setTotalNumber(record.getTotalNumber())
                .build();
    }

    private static RpcGetEphemeralsResponse toGetEphemeralsResponse(Chroot chroot, GetEphemeralsResponse record) {
        return RpcGetEphemeralsResponse.newBuilder()
                .addAllResult(record.getEphemerals())
                .build();
    }

    private static RpcGetACLResponse toGetACLResponse(Chroot chroot, GetACLResponse record) {
        return RpcGetACLResponse.newBuilder()
                .setStat(fromStat(record.getStat()))
                .setAcls(fromAcls(record.getAcl()))
                .build();
    }

    private static RpcSetACLResponse toSetACLResponse(Chroot chroot, SetACLResponse record) {
        return RpcSetACLResponse.newBuilder()
                .setResult(fromStat(record.getStat()))
                .build();
    }

    private static RpcTransactionResponse toTransactionResponse(Chroot chroot, MultiResponse record) {
        List<RpcTransactionResult> resultList = record.getResultList().stream()
                .map(r -> toOpResult(chroot, r))
                .collect(Collectors.toList());
        return RpcTransactionResponse.newBuilder()
                .addAllResult(resultList)
                .build();
    }

    private static RpcTransactionResult toCreateResult(Chroot chroot, OpResult.CreateResult record) {
        RpcTransactionCreateResult.Builder builder = RpcTransactionCreateResult.newBuilder();
        if (record.getStat() != null) {
            builder.setStat(fromStat(record.getStat()));
        }
        RpcTransactionCreateResult result = builder.setPath(chroot.unFixPath(record.getPath())).build();
        return RpcTransactionResult.newBuilder()
                .setCreate(result)
                .build();
    }

    private static RpcTransactionResult toDeleteResult(Chroot chroot, OpResult.DeleteResult record) {
        RpcTransactionDeleteResult result = RpcTransactionDeleteResult.newBuilder().build();
        return RpcTransactionResult.newBuilder()
                .setDelete(result)
                .build();
    }

    private static RpcTransactionResult toCheckResult(Chroot chroot, OpResult.CheckResult record) {
        RpcTransactionCheckResult result = RpcTransactionCheckResult.newBuilder().build();
        return RpcTransactionResult.newBuilder()
                .setCheck(result)
                .build();
    }

    private static RpcTransactionResult toSetDataResult(Chroot chroot, OpResult.SetDataResult record) {
        RpcTransactionSetDataResult result = RpcTransactionSetDataResult.newBuilder()
                .setStat(fromStat(record.getStat()))
                .build();
        return RpcTransactionResult.newBuilder()
                .setSetData(result)
                .build();
    }

    private static RpcTransactionResult toGetChildrenResult(Chroot chroot, OpResult.GetChildrenResult record) {
        RpcTransactionGetChildrenResult result = RpcTransactionGetChildrenResult.newBuilder()
                .addAllChildren(record.getChildren())
                .build();
        return RpcTransactionResult.newBuilder()
                .setGetChildren(result)
                .build();
    }

    private static RpcTransactionResult toGetDataResult(Chroot chroot, OpResult.GetDataResult record) {
        RpcTransactionGetDataResult result = RpcTransactionGetDataResult.newBuilder()
                .setStat(fromStat(record.getStat()))
                .setData(ByteString.copyFrom(record.getData()))
                .build();
        return RpcTransactionResult.newBuilder()
                .setGetData(result)
                .build();
    }

    private static RpcTransactionResult toErrorResult(Chroot chroot, OpResult.ErrorResult record) {
        RpcTransactionErrorResult result = RpcTransactionErrorResult.newBuilder()
                .setError(fromCode(record.getErr()))
                .build();
        return RpcTransactionResult.newBuilder()
                .setError(result)
                .build();
    }
}
