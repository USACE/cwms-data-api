package helpers;

import cwms.cda.api.OfficeController;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenApiTestHelper {


    public static List<Method> findByName(Class<?> klass, String name) {
        Method[] methods = klass.getMethods();
        return Stream.of(methods).filter(m -> m.getName().equals(name)).collect(toList());
    }

    public static Method findOneByName(Class<?> klass, String name) {
        List<Method> methods = findByName(klass, name);
        if (methods == null || methods.isEmpty()) {
            throw new RuntimeException("Did not find method with name " + name);
        }
        if (methods.size() > 1) {
            throw new RuntimeException("Multiple methods with name " + name);
        }
        return methods.get(0);
    }

    public static <T> OpenApiDocInfo<T> readDocParams(Class<T> clazz, Method m) {
        OpenApiDocInfo<T> info = new OpenApiDocInfo<>(clazz, m);
        OpenApi oa = m.getAnnotation(OpenApi.class);
        if (oa == null || oa.ignore()) {
            return info;
        }
        for (OpenApiParam p : oa.queryParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                info.getQueryParameters().add(p.name());
            }
        }
        for (OpenApiParam p : oa.pathParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                info.getPathParameters().add(p.name());
            }
        }
        return info;
    }

    private static String getFileNameWithoutExtension(Path path) {
        String fileName = path.toString()
                              .replace("cwms-data-api\\src\\main\\java\\", "")
                              .replace("src\\main\\java\\", "")
                              .replace("\\", ".");
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex > 0) ? fileName.substring(0, dotIndex) : fileName;
    }

    private static Class getClassFromName(String name, List<String> classesNotFound)
    {
        Class clazz = null;
        try {
            clazz = Class.forName(name);
        } catch (ClassNotFoundException ex) {
            classesNotFound.add(name);
        }
        return clazz;
    }

    public static <T> List<Class<T>> findClassesOfType(Class<T> type) {
        return findClassesOfType(type, "cwms.cda.api");
    }

    public static <T> List<Class<T>> findClassesOfType(Class<T> type, String packageName) {
        // Convert package name to path
        String packagePath = packageName.replace('.', '/');

        // Assuming the test is run from project root, adjust path as needed
        Path srcPath = Paths.get("cwms-data-api/src/main/java", packagePath);

        if (!Files.exists(srcPath)) {
            // Try alternative path
            srcPath = Paths.get("src/main/java", packagePath);
        }

        List<String> classesNotFound = new ArrayList<>();
        List<Class> temp;

        try {
            temp = Files.walk(srcPath)
                        .filter(Files::isRegularFile)
                        .map(OpenApiTestHelper::getFileNameWithoutExtension)
                        .map(name -> getClassFromName(name, classesNotFound))
                        .filter(clazz -> clazz != null && type.isAssignableFrom(clazz))
                        .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                        .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException("Error scanning for handler classes", e);
        }

        assertTrue(classesNotFound.isEmpty(), "Unable to find classes for " + String.join(", ", classesNotFound));
        List<Class<T>> output = new ArrayList<>();
        for (Class clazz : temp) {
            output.add((Class<T>)clazz);
        }
        return output;
    }

    public static <T> List<OpenApiDocInfo<T>> readOpenApiDocs(Class<? super T> baseClass, Class<T> primaryClass) {
        return Arrays.stream(baseClass.getDeclaredMethods())
                     .map(method -> findOneByName(primaryClass, method.getName()))
                     .map(method -> readDocParams(primaryClass, method))
                     .collect(toList());
    }
}
