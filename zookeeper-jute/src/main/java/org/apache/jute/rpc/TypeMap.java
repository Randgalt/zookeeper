package org.apache.jute.rpc;

import java.util.HashMap;
import java.util.Map;
import javax.lang.model.element.Element;

/**
 * Manages mapping from ZK/JDK types to their gRPC equivalents.
 */
class TypeMap {
    private final Map<String, String> messageTypeMap = new HashMap<>();

    /**
     * Add mapping from the given element to an RPC type.
     *
     * @param element the element
     * @param rpcType the RPC type name
     */
    void put(Element element, String rpcType) {
        put(elementType(element), rpcType);
    }

    /**
     * Add mapping from the given class to an RPC type.
     *
     * @param clazz   the class
     * @param rpcType the RPC type name
     */
    void put(Class<?> clazz, String rpcType) {
        messageTypeMap.put(clazz.getName(), rpcType);
    }

    /**
     * Add mapping from the given type to an RPC type.
     *
     * @param type    source type
     * @param rpcType the RPC type name
     */
    void put(String type, String rpcType) {
        messageTypeMap.put(type, rpcType);
    }

    /**
     * Return the mapped RPC type for the given element or {@code null}.
     *
     * @param element the element
     * @return RPC type name or {@code null}
     */
    String get(Element element) {
        return get(elementType(element));
    }

    /**
     * Return the mapped RPC type for the given class or {@code null}.
     *
     * @param clazz the class
     * @return RPC type name or {@code null}
     */
    String get(Class<?> clazz) {
        return get(clazz.getName());
    }

    /**
     * Return the mapped RPC type for the given type or {@code null}.
     *
     * @param type source type
     * @return RPC type name or {@code null}
     */
    String get(String type) {
        return messageTypeMap.get(type);
    }

    private static String elementType(Element element) {
        return element.asType().toString();
    }
}
