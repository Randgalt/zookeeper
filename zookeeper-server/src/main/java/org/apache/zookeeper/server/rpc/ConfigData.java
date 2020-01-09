package org.apache.zookeeper.server.rpc;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The reconfig/config methods return a serialized byte array. For gRPC we can return a fully defined config
 * record.  {@code JuteRpcProcessor} will replace the Jute definition with this.
 */
@SuppressWarnings("unused")
class ConfigData {
    static class MultipleAddress {
        Set<String> addresses;
        long timeoutMs;
    }

    static class Server {
        MultipleAddress addr;
        MultipleAddress electionAddr;
        String clientAddr;
        long id;
        String hostname;
        boolean isParticipant;
        boolean isClientAddrFromStatic;
        List<String> myAddrs;
    }

    Map<Long, Server> allMembers;
    Map<Long, Server> votingMembers;
    Map<Long, Server> observingMembers;
    long version;
    int half;
}
