package org.apache.jute.rpc;

import static org.apache.jute.rpc.Constants.PROTO_FILE_HEADER;
import static org.apache.jute.rpc.Constants.PROTO_FILE_NAME;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

/**
 * Annotation processor for {@link JuteRpc}. All methods annotated with {@link JuteRpc}
 * will be processed into gRPC/protobuf statements and written to the {@code zookeeper.proto} file.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class JuteRpcProcessor extends AbstractProcessor {
    private Builder builder;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        builder = new Builder(processingEnv);
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return new HashSet<>(Collections.singletonList(JuteRpc.class.getName()));
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        annotations.forEach(annotation -> roundEnv.getElementsAnnotatedWith(annotation).forEach(builder::addMethod));
        if (roundEnv.processingOver()) {
            writeFile();
        }
        return true;
    }

    private void writeFile() {
        try {
            FileObject file = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "", PROTO_FILE_NAME);
            try (PrintWriter out = new PrintWriter(file.openWriter())) {
                out.print(PROTO_FILE_HEADER);
                builder.streamStatements().forEach(out::println);
            }
            processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Protobuf file written to: " + file.getName());
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Could not write protobuf file to: " + PROTO_FILE_NAME);
        }
    }
}
