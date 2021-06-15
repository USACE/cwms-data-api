package service.annotations;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import com.google.auto.service.AutoService;

@SupportedAnnotationTypes("service.annotations.FormatService")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class FormatServiceProcessor extends AbstractProcessor{

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {            
        
        try {
            
        
            System.out.println("Processing annotations***************");
            System.err.println("Processing annotations");
            for(TypeElement anno: annotations){
                System.out.println(anno.toString());
                Collection<? extends Element> elements = roundEnv.getElementsAnnotatedWith(anno);
                FileObject createResource = processingEnv.getFiler().createResource(StandardLocation.SOURCE_OUTPUT, "", "formats.list");
                Writer wr = createResource.openWriter();
                elements.forEach( element -> {
                    System.out.println(element.toString());
                    FormatService fs = element.getAnnotation(FormatService.class);
                    System.out.println(fs.contentType());
                    try {
                        wr.write(fs.contentType()+":");
                        wr.write(element.asType().toString()+":");                        
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    List<? extends AnnotationMirror> mirrors = element.getAnnotationMirrors();
                    for(AnnotationMirror mirror: mirrors){
                        System.out.println("\t"+mirror.toString());
                        Map<? extends ExecutableElement, ?extends AnnotationValue> ev = mirror.getElementValues();
                        for(Map.Entry<? extends ExecutableElement, ?extends AnnotationValue> entry: ev.entrySet()){
                            String key = entry.getKey().getSimpleName().toString();
                            Object value = entry.getValue().getValue();
                            switch(key){
                                case "dataTypes": 
                                    @SuppressWarnings("unchecked")
                                    List<? extends AnnotationValue> typeMirrors= (List<? extends AnnotationValue>) value;
                                    typeMirrors.forEach( tm -> {
                                        System.out.println("\t"+ tm.getValue().toString());
                                        try {
                                            wr.write( tm.getValue().toString() + ";");
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    });
                                    
                                    break;
                            }                            
                        }                        
                    }
                    try {
                        wr.write("\r\n");
                    } catch (IOException e) {                        
                        e.printStackTrace();
                    }

                });      
                wr.close();
            }
            
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    
}
