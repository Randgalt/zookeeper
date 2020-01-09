package org.apache.jute.rpc;

import static org.apache.jute.rpc.Constants.ACL_LIST_CLASS_NAME;
import static org.apache.jute.rpc.Constants.RPC_ACL_LIST_CLASS_NAME;
import static org.apache.jute.rpc.Constants.STAT_CLASS_NAME;
import static org.apache.jute.rpc.Constants.SUBSTITUTE_CONFIG_DATA_CLASS_NAME;
import static org.apache.jute.rpc.JuteRpcSpecialHandling.STAT_RESULT;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;

/**
 * Utilities for listing element sub-elements while doing required substitutions, etc.
 */
class ElementUtil {
    private final Map<String, Element> substitutions;
    private final Element substituteConfigDataElement;
    private final ProcessingEnvironment processingEnv;

    ElementUtil(ProcessingEnvironment processingEnv) {
        substituteConfigDataElement = processingEnv.getElementUtils().getTypeElement(SUBSTITUTE_CONFIG_DATA_CLASS_NAME);
        this.processingEnv = processingEnv;

        Map<String, Element> work = new HashMap<>();
        work.put(ACL_LIST_CLASS_NAME, processingEnv.getElementUtils().getTypeElement(RPC_ACL_LIST_CLASS_NAME));
        substitutions = Collections.unmodifiableMap(work);
    }

    /**
     * Returns the special {@code ConfigData} element.
     *
     * @return {@code ConfigData} element
     */
    Element getSubstituteConfigDataElement() {
        return substituteConfigDataElement;
    }

    /**
     * Return the parameter elements of the given executable element.
     *
     * @param element  element
     * @param specials annotated specials on the method
     * @return elements
     */
    List<? extends Element> parameters(ExecutableElement element, Collection<JuteRpcSpecialHandling> specials) {
        if (specials.contains(STAT_RESULT)) {
            return element.getParameters().stream()
                    .filter(e -> !e.asType().toString().equals(STAT_CLASS_NAME))
                    .map(this::doSubstitutions)
                    .collect(Collectors.toList());
        }
        return element.getParameters()
                .stream()
                .map(this::doSubstitutions)
                .collect(Collectors.toList());
    }

    /**
     * Return the usable fields from a class.
     *
     * @param element element of the class
     * @return list of field elements
     */
    List<Element> messageFields(Element element) {
        List<? extends Element> enclosedElements = getEnclosedElements(element);
        return enclosedElements.stream()
                .filter(e -> e.getKind() == ElementKind.FIELD)
                .filter(e -> !e.getModifiers().contains(Modifier.STATIC))
                .map(this::doSubstitutions)
                .collect(Collectors.toList());
    }

    /**
     * Return the usable enum components from an enum.
     *
     * @param element element of the enum
     * @return list of enum components
     */
    List<Element> enumElements(Element element) {
        return getEnclosedElements(element).stream()
                .filter(e -> e.getKind() == ElementKind.ENUM_CONSTANT)
                .map(this::doSubstitutions)
                .collect(Collectors.toList());
    }

    /**
     * Return the usable elements from a {@code ZooDef}.
     *
     * @param element ZooDef element
     * @return list elements
     */
    List<Element> zooDefsElements(Element element) {
        return getEnclosedElements(element).stream()
                .filter(e -> e.getKind() == ElementKind.FIELD)
                .filter(e -> e.getModifiers().contains(Modifier.STATIC) && e.getModifiers().contains(Modifier.PUBLIC))
                .map(this::doSubstitutions)
                .collect(Collectors.toList());
    }

    /**
     * For an executable element, return its "return type" element.
     *
     * @param element method
     * @return return type element
     */
    Element returnElement(ExecutableElement element) {
        return doSubstitutions(processingEnv.getTypeUtils().asElement(element.getReturnType()), element.getReturnType());
    }

    /**
     * Returns the correct "return type". If {@code returnElement} is a substution, it's type
     * is returned. Otherwise, the main element's return type is returned.
     *
     * @param mainElement   main method
     * @param returnElement the return element
     * @return the correct return type
     */
    TypeMirror getReturnType(ExecutableElement mainElement, Element returnElement) {
        if (substitutions.containsValue(returnElement)) {
            return returnElement.asType();
        }
        return mainElement.getReturnType();
    }

    /**
     * Due to vagaries of the annotation processor AST, we need to do a dance to
     * get at an elements enclosed fields/etc.
     *
     * @param element element
     * @return enclosed elements
     */
    private List<? extends Element> getEnclosedElements(Element element) {
        return processingEnv.getTypeUtils().asElement(element.asType()).getEnclosedElements();
    }

    private Element doSubstitutions(Element element) {
        return doSubstitutions(element, element.asType());
    }

    private Element doSubstitutions(Element element, TypeMirror type) {
        return substitutions.getOrDefault(type.toString(), element);
    }
}
