package org.apache.jute.rpc;

import static org.apache.jute.rpc.Constants.KEEPER_EXCEPTION_CODE_CLASS_NAME;
import static org.apache.jute.rpc.Constants.MIN_FIELD_NUMBER;
import static org.apache.jute.rpc.Constants.ONE_OF_NAME;
import static org.apache.jute.rpc.Constants.REQUEST;
import static org.apache.jute.rpc.Constants.RESPONSE;
import static org.apache.jute.rpc.Constants.SPECIAL_XIDS;
import static org.apache.jute.rpc.Constants.SPECIAL_XID_PREFIX;
import static org.apache.jute.rpc.Constants.STAT;
import static org.apache.jute.rpc.Constants.STAT_CLASS_NAME;
import static org.apache.jute.rpc.Constants.UNKNOWN;
import static org.apache.jute.rpc.Constants.ZOOKEEPER_SERVICE_NAME;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.CONFIG_DATA_RESULT;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.CREATE_EMPTY_REQUEST;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.INLINE_RETURN_TYPE;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.STAT_RESULT;
import static org.apache.jute.rpc.Util.capitalize;
import static org.apache.jute.rpc.Util.constantNaming;
import static org.apache.jute.rpc.Util.getAnnotation;
import static org.apache.jute.rpc.Util.getClassSimpleName;
import static org.apache.jute.rpc.Util.getSpecials;
import static org.apache.jute.rpc.Util.rpc;
import static org.apache.jute.rpc.Util.unCapitalize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

/**
 * Uses the annotation processing AST to convert annotated ZooKeeper methods/classes/enums into gRPC/protobuf
 * statements. The implementation is exhaustive enough for currently known ZooKeeper methods and data structures
 * but may not handle future versions.
 */
class Builder {
    private final TypeMap typeMap = new TypeMap();
    private final List<String> messages = new ArrayList<>();
    private final Map<Integer, TypeAndName> requestMap = new TreeMap<>();
    private final Map<Integer, TypeAndName> responseMap = new TreeMap<>();
    private final ElementUtil elementUtil;
    private final ProcessingEnvironment processingEnv;

    private static final Collection<String> collectionTypes = Collections.unmodifiableCollection(Arrays.asList(
            List.class.getName(),
            Collection.class.getName(),
            Set.class.getName()
    ));
    private static final Collection<String> mapTypes = Collections.unmodifiableCollection(Arrays.asList(
            Map.class.getName(),
            HashMap.class.getName(),
            TreeMap.class.getName()
    ));

    private static class TypeAndName {
        private final String type;
        private final String name;

        TypeAndName(String type, String name) {
            this.type = type;
            this.name = name;
        }
    }

    Builder(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
        elementUtil = new ElementUtil(processingEnv);

        // pre fill type map with primitives, strings, etc.
        typeMap.put(String.class, "string");
        typeMap.put(Short.TYPE, "int32");
        typeMap.put(Short.class, "int32");
        typeMap.put(Character.TYPE, "int32");
        typeMap.put(Character.class, "int32");
        typeMap.put(Integer.TYPE, "int32");
        typeMap.put(Integer.class, "int32");
        typeMap.put(Long.TYPE, "int64");
        typeMap.put(Long.class, "int64");
        typeMap.put(Boolean.TYPE, "bool");
        typeMap.put(Boolean.class, "bool");
        typeMap.put(Float.TYPE, "float");
        typeMap.put(Float.class, "float");
        typeMap.put(Double.TYPE, "double");
        typeMap.put(Double.class, "double");
        typeMap.put("byte[]", "bytes");
    }

    /**
     * Return a stream of gRPC/protobuf statements.
     *
     * @return stream of gRPC/protobuf statements
     */
    Stream<String> streamStatements() {
        Stream<String> serviceMessages = Stream.of(
                formatSpecialXids(),
                formatRequest(),
                formatResponse(),
                formatService()
        );
        return Stream.concat(messages.stream(), serviceMessages);
    }

    /**
     * Process an annotated Java method into a gRPC/protobuf statement.
     *
     * @param element annotated method
     */
    void addMethod(Element element) {
        buildMethod((ExecutableElement) element);
    }

    private String formatSpecialXids() {
        String prefix = constantNaming(SPECIAL_XID_PREFIX) + "_";
        ProtobufFormatter message = ProtobufFormatter.startEnum(rpc(SPECIAL_XIDS));
        message.enumField(prefix + UNKNOWN, 0);
        message.enumField(prefix + "NOTIFICATION_XID", -1);
        message.enumField(prefix + "PING_XID", -2);
        message.enumField(prefix + "AUTHPACKET_XID", -4);
        message.enumField(prefix + "SET_WATCHES_XID", -8);
        return message.finish();
    }

    /**
     * Generate the gRPC service statement. Because of message ordering/async requirements there is only 1 gRPC
     * service. E.g.
     *
     * <pre> {@code
     * service ZooKeeperService {
     *     rpc process (stream RpcRequest) returns (stream RpcResponse);
     * }
     * }</pre>
     *
     * @return the protobuf statement
     */
    private String formatService() {
        String requestName = rpc(REQUEST);
        String responseName = rpc(RESPONSE);
        return ProtobufFormatter.startService(ZOOKEEPER_SERVICE_NAME)
                .serviceMethod(requestName, responseName)
                .finish();
    }

    /**
     * Generate the {@code RpcRequest}. These protobuf messages consist
     * of "header" values followed by a {@code oneof} of each of the generated requests. The ZooKeeper
     * server and gRPC clients stream these requests. E.g.
     *
     * <pre> {@code
     * message RpcRequest {
     *     int64 xid = 2;
     *     oneof message {
     *         RpcConnectRequest connect = 12;
     *         RpcTransactionRequest transaction = 13;
     *         RpcFourLetterRequest fourLetter = 14;
     *         ...
     *     }
     * }
     * }</pre>
     *
     * @return the protobuf statement
     */
    private String formatRequest() {
        ProtobufFormatter message = ProtobufFormatter.startMessage(rpc(REQUEST))
                .field("xid", 1, typeMap.get(Long.TYPE))
                .startOneOf(ONE_OF_NAME);
        requestMap.forEach((fieldNumber, typeAndName) -> message.field(typeAndName.name, fieldNumber, typeAndName.type));
        message.endOneOf();
        return message.finish();
    }

    /**
     * Generate the {@code RpcResponse}. These protobuf messages consist
     * of "header" values followed by a {@code oneof} of each of the generated responses. The ZooKeeper
     * server and gRPC clients stream these requests. E.g.
     *
     * <pre> {@code
     * message RpcResponse {
     *     int64 xid = 2;
     *     int64 zxid = 3;
     *     RpcKeeperExceptionCode error = 4;
     *     oneof message {
     *         RpcWatcherEventResponse watcherEvent = 10;
     *         RpcErrorResultResponse errorResult = 11;
     *         ...
     *    }
     * }
     * }</pre>
     *
     * @return the protobuf statement
     */
    private String formatResponse() {
        ProtobufFormatter message = ProtobufFormatter.startMessage(rpc(RESPONSE))
                .field("xid", 1, typeMap.get(Long.TYPE))
                .field("zxid", 2, typeMap.get(Long.TYPE))
                .field("error", 3, typeMap.get(KEEPER_EXCEPTION_CODE_CLASS_NAME))
                .startOneOf(ONE_OF_NAME);
        responseMap.forEach((fieldNumber, typeAndName) -> message.field(typeAndName.name, fieldNumber, typeAndName.type));
        message.endOneOf();
        return message.finish();
    }

    /**
     * Generate a protobuf enum.
     *
     * @param baseName          the name of the field, variable, parameter etc
     * @param enumName          enum type name
     * @param enumConstantNames the enum constant names
     * @return the protobuf statement
     */
    private String formatEnum(String baseName, String enumName, List<String> enumConstantNames) {
        List<String> formattedNames = enumConstantNames.stream().map(Util::constantNaming).collect(Collectors.toList());
        String prefix = constantNaming(baseName) + "_";
        ProtobufFormatter message = ProtobufFormatter.startEnum(enumName);
        boolean hasUnknownAlready = formattedNames.contains(UNKNOWN);
        if (!hasUnknownAlready) {
            message.enumField(prefix + UNKNOWN, 0);
        }

        int enumFieldNumber = hasUnknownAlready ? 0 : 1;
        for (String formattedName : formattedNames) {
            message.enumField(prefix + formattedName, enumFieldNumber++);
        }
        return message.finish();
    }

    /**
     * Check the typeMap for an existing mapping and return if found. Otherwise call {@link #buildType(Element)}
     * and return the new mapping.
     *
     * @param mainElement main element
     * @param typeElement the type to get or build
     * @param type        its type mirror
     * @return the mapped type
     */
    private String getOrBuildType(Element mainElement, Element typeElement, TypeMirror type) {
        String rpcType = (typeElement != null) ? typeMap.get(typeElement) : typeMap.get(type.toString());
        if (rpcType == null) {
            // no current mapping - pass to buildType() to generate a new protobuf message and store the mapping
            if (typeElement == null) {
                error(mainElement, "Unsupported type: %s", type.toString());
            } else {
                rpcType = buildType(typeElement);
            }
        }
        return rpcType;
    }

    /**
     * Build the given element into a gRPC/protobuf statement and then store that new statement in the type map
     * so that it can be re-used if it's encountered again.
     *
     * @param element the element
     * @return the protobuf statement name
     */
    private String buildType(Element element) {
        String name = messageName(element);
        BuildType buildType = BuildType.get(processingEnv, element);
        switch (buildType) {
            case ENUM:
                List<? extends Element> elements = elementUtil.enumElements(element);
                buildEnum(element, name, elements);
                break;

            case ZOO_DEF:
                List<Element> zooDefs = elementUtil.zooDefsElements(element);
                buildEnum(element, messageName(element), zooDefs);
                break;

            case ONE_OF:
                buildOneOfMessage(name, unCapitalize(element.getSimpleName().toString()), elementUtil.messageFields(element));
                break;

            default:
            case MESSAGE:
                buildMessage(name, elementUtil.messageFields(element));
                break;
        }
        typeMap.put(element, name);
        return name;
    }

    /**
     * Build a Java method into a gRPC statement. The method parameters (if any) are generated into a new protobuf message
     * named {@code Rpc<MethodName>Request}. The return value (if not {@code void}) is generated into a new protobuf
     * message named {@code Rpc<MethodName>Response}. The new response/request are added to the requestMap/responseMap
     * so that the final zookeeper.proto's {@code RpcRequestType}, {@code RpcResponseType}, {@code RpcRequest} and
     * {@code RpcResponse} contain references.
     *
     * @param element method element
     */
    private void buildMethod(ExecutableElement element) {
        String name = messageName(element);

        Collection<JuteRpcSpecialHandling> specials = getSpecials(element);

        if (element.getReturnType().getKind() != TypeKind.VOID) {
            Element returnElement;
            if (specials.contains(CONFIG_DATA_RESULT)) {
                returnElement = elementUtil.getSubstituteConfigDataElement();
            } else if (element.getReturnType().getKind() == TypeKind.ARRAY) {
                returnElement = null;
            } else if (element.getReturnType().getKind().isPrimitive()) {
                returnElement = processingEnv.getTypeUtils().boxedClass((PrimitiveType) element.getReturnType());
            } else {
                returnElement = elementUtil.returnElement(element);
            }
            String responseName = name + RESPONSE;

            ProtobufFormatter responseMessage = ProtobufFormatter.startMessage(responseName);
            int fieldNumber;
            if ((returnElement != null) && specials.contains(INLINE_RETURN_TYPE)) {
                fieldNumber = addMessageElements(responseMessage, elementUtil.messageFields(returnElement));
            } else {
                TypeMirror returnType = elementUtil.getReturnType(element, returnElement);
                addMessageElement(responseMessage, getAnnotation(element).responseName(), returnElement, returnType, 1);
                fieldNumber = 2;
            }
            if (specials.contains(STAT_RESULT)) {
                element.getParameters().stream()
                        .filter(e -> e.asType().toString().equals(STAT_CLASS_NAME))
                        .findFirst()
                        .ifPresent(e -> addMessageElement(responseMessage, STAT, e, e.asType(), fieldNumber));
            }
            finishAndStoreMessage(responseMessage);
            addToRequestResponseMap(responseMap, responseName, element);
        }

        List<? extends Element> parameters = elementUtil.parameters(element, specials);
        if (!parameters.isEmpty() || specials.contains(CREATE_EMPTY_REQUEST)) {
            String requestName = name + REQUEST;
            buildMessage(requestName, parameters);
            addToRequestResponseMap(requestMap, requestName, element);
        }
    }

    /**
     * Build a protobuf enum.
     *
     * @param mainElement      the source element for the enum
     * @param name             the enum name
     * @param enclosedElements enum elements
     */
    private void buildEnum(Element mainElement, String name, List<? extends Element> enclosedElements) {
        List<String> enclosedElementNames = enclosedElements.stream()
                .map(element -> element.getSimpleName().toString())
                .collect(Collectors.toList());
        TypeMirror erasedType = processingEnv.getTypeUtils().erasure(mainElement.asType());
        String message = formatEnum(getClassSimpleName(erasedType), name, enclosedElementNames);
        messages.add(message);
    }

    /**
     * Build a protobuf message for a Java class.
     *
     * @param name     message name
     * @param elements class elements
     */
    private void buildMessage(String name, List<? extends Element> elements) {
        ProtobufFormatter message = ProtobufFormatter.startMessage(name);
        addMessageElements(message, elements);
        finishAndStoreMessage(message);
    }

    /**
     * Build a protobuf message for a Java class where each field is a {@code oneof}.
     *
     * @param name      message name
     * @param oneOfName the name for the oneof
     * @param elements  class elements
     */
    private void buildOneOfMessage(String name, String oneOfName, List<? extends Element> elements) {
        ProtobufFormatter message = ProtobufFormatter.startMessage(name)
                .startOneOf(oneOfName);
        addMessageElements(message, elements);
        message.endOneOf();
        finishAndStoreMessage(message);
    }

    /**
     * Generate the name for a protobuf message. For methods, it's the capitalized method name. Otherwise it's the type's
     * class simple name.
     *
     * @param element element
     * @return name
     */
    private String messageName(Element element) {
        String baseName;
        if (element.getKind() == ElementKind.METHOD) {
            String name = element.getSimpleName().toString();
            baseName = capitalize(name);
        } else {
            TypeMirror erasedType = processingEnv.getTypeUtils().erasure(element.asType());
            baseName = getClassSimpleName(erasedType);
            Element typeElement = processingEnv.getTypeUtils().asElement(erasedType);
            if (typeElement != null) {
                // the class is nested in another class - include the parent class name in the message name
                if (typeElement.getEnclosingElement().getKind() == ElementKind.CLASS) {
                    baseName = typeElement.getEnclosingElement().getSimpleName().toString() + baseName;
                }
            }
        }
        return rpc(baseName);
    }

    /**
     * Add a newly generated request/response to the either the {@code requestMap} or {@code responseMap}.
     *
     * @param map     either the {@code requestMap} or {@code responseMap}
     * @param name    protobuf type name
     * @param element source element
     */
    private void addToRequestResponseMap(Map<Integer, TypeAndName> map, String name, Element element) {
        int fieldNumber = getAnnotation(element).value();
        if (fieldNumber < MIN_FIELD_NUMBER) {
            error(element, "The @JuteRpc values from 1 to %d are reserved for internal use.", MIN_FIELD_NUMBER - 1);
        } else {
            if (map.containsKey(fieldNumber)) {
                error(element, "@JuteRpc values must be unique. %d was already used.", fieldNumber);
            } else {
                map.put(fieldNumber, new TypeAndName(name, element.getSimpleName().toString()));
            }
        }
    }

    /**
     * Add all the given elements to the currently formatting message.
     *
     * @param message  message formatter
     * @param elements elements
     * @return the next field number
     */
    private int addMessageElements(ProtobufFormatter message, List<? extends Element> elements) {
        int fieldNumber = 1;
        for (Element element : elements) {
            String elementName = unCapitalize(element.getSimpleName().toString());
            addMessageElement(message, elementName, element, element.asType(), fieldNumber++);
        }
        return fieldNumber;
    }

    /**
     * Add a field/value/etc. to a currently building protobuf message. This method does most of the work in this class.
     * It teases out what type of element it is: standard, generic/parameterized, etc. to get a master "source type".
     * If there isn't an existing mapping for the source type, it is passed to {@link #buildType(Element)} to generate a
     * protobuf message for the type. Finally, the new message line is added to the currently formatting message.
     *
     * @param message     message being formatted
     * @param name        the field name
     * @param element     element to add
     * @param type        the element's type
     * @param fieldNumber the field number to use
     */
    private void addMessageElement(ProtobufFormatter message, String name, Element element, TypeMirror type, int fieldNumber) {
        boolean repeated = false;
        String mapKeyType = null;
        Element thisElement = element;
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType declaredType = (DeclaredType) type;
            TypeMirror erasedType;
            switch (declaredType.getTypeArguments().size()) {
                case 0:
                    // nop
                    break;

                case 1:
                    // it's a single parameterized type. Currently only collection types are supported. Pull out
                    // the collection type argument and use that as the source type and note that it should
                    // be a protobuf "repeated" field
                    erasedType = processingEnv.getTypeUtils().erasure(declaredType);
                    if (collectionTypes.contains(erasedType.toString())) {
                        thisElement = processingEnv.getTypeUtils().asElement(declaredType.getTypeArguments().get(0));
                        repeated = true;
                    } else {
                        error(element, "Unsupported collection type: %s", erasedType.toString());
                    }
                    break;

                case 2:
                    // it's a double parameterized type. Currently only map types are supported. Pull out
                    // the key and value type arguments and create a protobuf map field
                    erasedType = processingEnv.getTypeUtils().erasure(declaredType);
                    if (mapTypes.contains(erasedType.toString())) {
                        Element keyElement = processingEnv.getTypeUtils().asElement(declaredType.getTypeArguments().get(0));
                        thisElement = processingEnv.getTypeUtils().asElement(declaredType.getTypeArguments().get(1));
                        mapKeyType = getOrBuildType(element, keyElement, keyElement.asType());
                    } else {
                        error(element, "Unsupported map type: %s", erasedType.toString());
                    }
                    break;

                default:
                    error(element, "Unsupported collection type: %s", declaredType.toString());
                    break;
            }
        }

        String rpcType = getOrBuildType(element, thisElement, type);
        if (mapKeyType != null) {
            message.map(name, fieldNumber, mapKeyType, rpcType);
        } else if (repeated) {
            message.repeatedField(name, fieldNumber, rpcType);
        } else {
            message.field(name, fieldNumber, rpcType);
        }
    }

    /**
     * Finish the currently building message and store it in the messages list.
     *
     * @param message message
     */
    private void finishAndStoreMessage(ProtobufFormatter message) {
        String messageStr = message.finish();
        messages.add(messageStr);
    }

    /**
     * Print an error diagnostic.
     *
     * @param e      element
     * @param format format string
     * @param args   arguments
     */
    private void error(Element e, String format, Object... args) {
        String message = String.format(format, args);
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, e);
    }
}
