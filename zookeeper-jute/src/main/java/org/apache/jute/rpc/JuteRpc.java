package org.apache.jute.rpc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method in the ZooKeeper client code as being part of the gRPC protobuf definition.
 * {@link JuteRpcProcessor} will process the method and add the protobuf IDL to the protobuf file.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface JuteRpc {
    /**
     * The protobuf field number. This is required and once specified it must NEVER be changed again. gRPC/protobuf uses
     * this field number as method identifiers. If field numbers were to change between ZooKeeper versions, backward
     * compatibility would be lost. Note: field numbers 1-9 are reserved for internal use.
     *
     * @return protobuf field number
     */
    int value();

    /**
     * Method returns are wrapped in a protobuf message. The {@code responseName} is the name of the
     * message field.
     *
     * @return name to use for return field
     */
    String responseName() default "result";

    /**
     * Mechanism to work around quirks, etc. for some of the ZooKeeper methods
     *
     * @return special handling mode - default is none
     */
    JuteRpcSpecialHandling[] specialHandling() default {};
}
