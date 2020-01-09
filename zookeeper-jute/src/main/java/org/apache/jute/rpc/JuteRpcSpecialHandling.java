package org.apache.jute.rpc;

/**
 * Mechanism to work around quirks, etc. for some of the ZooKeeper methods.
 */
public enum JuteRpcSpecialHandling {
    /**
     * Some ZooKeeper client methods pass a {@code Stat} instance that gets filled with the value returned
     * from the server. This doesn't work with gRPC. {@code STAT_RESULT} enables workaround behavior for
     * these methods.
     */
    STAT_RESULT,

    /**
     * The reconfig/config methods return a serialized byte array. For gRPC we can return a fully defined config
     * record.
     */
    CONFIG_DATA_RESULT,

    /**
     * Normally, a separate message type is created for the method return type. If this option is added,
     * the fields of the return type are inlined and a new protobuf message is not created.
     */
    INLINE_RETURN_TYPE,

    /**
     * Normally, methods without parameters do not generate a request message. This option causes an empty
     * request message to be generated.
     */
    CREATE_EMPTY_REQUEST
}
