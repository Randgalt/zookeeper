package org.apache.jute.rpc;

import static org.apache.jute.rpc.Constants.RPC_PREFIX;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import javax.lang.model.element.Element;
import javax.lang.model.type.TypeMirror;

class Util {
    /**
     * Capitalize the first character of the string.
     *
     * @param s string to capitalize
     * @return capitalized string
     */
    static String capitalize(String s) {
        if (s.length() < 1) {
            return s.toUpperCase();
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    /**
     * Un-capitalize the first character of the string.
     *
     * @param s string to un-capitalize
     * @return Un-capitalized string
     */
    static String unCapitalize(String s) {
        if (s.length() < 1) {
            return s.toLowerCase();
        }
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    /**
     * Format the given string with standard enum constant casing.
     *
     * @param s string to format
     * @return formatted string
     */
    static String constantNaming(String s) {
        StringBuilder str = new StringBuilder(s.length() * 2);
        char previousChar = '_';
        for (char c : s.toCharArray()) {
            boolean isUpper = Character.isUpperCase(c);
            if (isUpper && !Character.isUpperCase(previousChar) && (previousChar != '_')) {
                str.append('_');
            }
            str.append(Character.toUpperCase(c));
            previousChar = c;
        }
        return str.toString();
    }

    /**
     * String format with a newline.
     *
     * @param str    string builder
     * @param format format string
     * @param args   arguments
     */
    static void appendLine(StringBuilder str, String format, Object... args) {
        str.append(String.format(format + "\n", args));
    }

    /**
     * Prefix with "Rpc".
     *
     * @param s string to prefix
     * @return prefixed string
     */
    static String rpc(String s) {
        return RPC_PREFIX + s;
    }

    /**
     * Convert any {@link JuteRpc} special handlings to a collection.
     *
     * @param element element
     * @return specials
     */
    static Collection<JuteRpcSpecialHandling> getSpecials(Element element) {
        JuteRpc annotation = getAnnotation(element);
        if (annotation.specialHandling().length > 0) {
            return Collections.unmodifiableCollection(Arrays.asList(annotation.specialHandling()));
        }
        return Collections.emptySet();
    }

    /**
     * Return the {@link JuteRpc} annotation for the given element.
     *
     * @param element element
     * @return annotation
     */
    static JuteRpc getAnnotation(Element element) {
        JuteRpc annotation = element.getAnnotation(JuteRpc.class);
        if (annotation == null) {
            throw new IllegalStateException("@JuteRpc annotation not found on: " + element);
        }
        return annotation;
    }

    /**
     * Given an erased type, return the class's simple name.
     *
     * @param erasedType type
     * @return simple name
     */
    static String getClassSimpleName(TypeMirror erasedType) {
        String simpleName = erasedType.toString();
        return simpleName.substring(simpleName.lastIndexOf(".") + 1); // copied from Class#getSimpleName()
    }
}
