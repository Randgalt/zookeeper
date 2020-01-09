package org.apache.jute.rpc;

import static org.apache.jute.rpc.Constants.INDENT;
import static org.apache.jute.rpc.Constants.MAIN_SERVICE_METHOD_NAME;
import static org.apache.jute.rpc.Util.appendLine;

/**
 * Utility for formatting protobuf statements. Chain various methods
 * and call {@link ProtobufFormatter#finish()} to complete and return the
 * generated statement.
 */
@SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
class ProtobufFormatter {
    private final StringBuilder message = new StringBuilder();
    private int indent = 0;

    /**
     * Start a new message.
     *
     * @param name message name
     * @return formatter
     */
    static ProtobufFormatter startMessage(String name) {
        return new ProtobufFormatter("message", name);
    }

    /**
     * Start a new enum.
     *
     * @param name enum name
     * @return formatter
     */
    static ProtobufFormatter startEnum(String name) {
        return new ProtobufFormatter("enum", name);
    }

    /**
     * Start a new service.
     *
     * @param name service name
     * @return formatter
     */
    static ProtobufFormatter startService(String name) {
        return new ProtobufFormatter("service", name);
    }

    /**
     * Add a protobuf field.
     *
     * @param name        field name
     * @param fieldNumber field number
     * @param typeName    the protobuf type name
     * @return this
     */
    ProtobufFormatter field(String name, int fieldNumber, String typeName) {
        append("%s %s = %d;", typeName, name, fieldNumber);
        return this;
    }

    /**
     * Add a protobuf repeated field.
     *
     * @param name        field name
     * @param fieldNumber field number
     * @param typeName    the protobuf type name
     * @return this
     */
    ProtobufFormatter repeatedField(String name, int fieldNumber, String typeName) {
        append("repeated %s %s = %d;", typeName, name, fieldNumber);
        return this;
    }

    /**
     * Add a protobuf map field.
     *
     * @param name         field name
     * @param fieldNumber  field number
     * @param keyRpcType   the protobuf type name of the key
     * @param valueRpcType the protobuf type name of the value
     * @return this
     */
    ProtobufFormatter map(String name, int fieldNumber, String keyRpcType, String valueRpcType) {
        append("map<%s, %s> %s = %d;", keyRpcType, valueRpcType, name, fieldNumber);
        return this;
    }

    /**
     * Add a protobuf enum field name.
     *
     * @param name        enum name
     * @param fieldNumber field number
     * @return this
     */
    ProtobufFormatter enumField(String name, int fieldNumber) {
        append("%s = %d;", name, fieldNumber);
        return this;
    }

    /**
     * Add a protobuf service.
     *
     * @param requestName  the name of the request
     * @param responseName the name of the response
     * @return this
     */
    ProtobufFormatter serviceMethod(String requestName, String responseName) {
        append("rpc %s (stream %s) returns (stream %s);", MAIN_SERVICE_METHOD_NAME, requestName, responseName);
        return this;
    }

    /**
     * Start a {@code oneof} section.
     *
     * @param oneOfName the name of the oneof
     * @return this
     */
    ProtobufFormatter startOneOf(String oneOfName) {
        append("oneof %s {", oneOfName);
        ++indent;
        return this;
    }

    /**
     * Finish a {@code oneof} section.
     *
     * @return this
     */
    ProtobufFormatter endOneOf() {
        --indent;
        append("}");
        return this;
    }

    /**
     * Complete and return the statement.
     *
     * @return completed statement
     */
    String finish() {
        message.append("}\n");
        return message.toString();
    }

    private ProtobufFormatter(String type, String name) {
        append("%s %s {", type, name);
        ++indent;
    }

    private void append(String format, Object... args) {
        for (int i = 0; i < indent; ++i) {
            message.append(INDENT);
        }
        appendLine(message, format, args);
    }
}
