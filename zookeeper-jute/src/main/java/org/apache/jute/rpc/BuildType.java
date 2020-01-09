package org.apache.jute.rpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;

enum BuildType {
    ENUM,
    MESSAGE,
    ZOO_DEF,
    ONE_OF;

    private static final Map<String, BuildType> specials;

    static {
        Map<String, BuildType> work = new HashMap<>();
        work.put(Constants.IDS_CLASS_NAME, ZOO_DEF);
        work.put(Constants.PERMS_CLASS_NAME, ZOO_DEF);
        work.put(Constants.TRANSACTION_OPERATION_CLASS_NAME, ONE_OF);
        work.put(Constants.TRANSACTION_RESULT_CLASS_NAME, ONE_OF);
        specials = Collections.unmodifiableMap(work);
    }

    /**
     * Return the element's type for building purposes. The type determines
     * what gRPC/protobuf statement type is generated and which element components
     * are used.
     *
     * @param processingEnv current processing environment
     * @param element       the element
     * @return type
     */
    static BuildType get(ProcessingEnvironment processingEnv, Element element) {
        if (isEnum(processingEnv, element)) {
            return ENUM;
        }
        return specials.getOrDefault(element.asType().toString(), MESSAGE);
    }

    private static boolean isEnum(ProcessingEnvironment processingEnv, Element element) {
        return processingEnv.getTypeUtils().directSupertypes(element.asType())
                .stream()
                .map(t -> processingEnv.getTypeUtils().erasure(t))
                .anyMatch(t -> t.toString().equals(Enum.class.getName()));
    }
}
