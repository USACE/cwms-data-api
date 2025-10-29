package helpers;

import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import org.junit.jupiter.api.Assertions;
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

    public static OpenApiDocInfo readDocParams(Method m) {
        OpenApiDocInfo info = new OpenApiDocInfo();
        OpenApi oa = m.getAnnotation(
                OpenApi.class);
        if (oa == null || oa.ignore()) {
            return info;
        }
        for (OpenApiParam p : oa.queryParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                info.query.add(p.name());
            }
        }
        for (OpenApiParam p : oa.pathParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                info.path.add(p.name());
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

    public static <T> List<Class<T>> findCrudHandlerClasses(Class<T> type) {
        return findHandlerClassNames(type, "cwms.cda.api");
    }

    public static <T> List<Class<T>> findHandlerClassNames(Class<T> type, String packageName) {
        List<String> handlerClasses = new ArrayList<>();

        // Convert package name to path
        String packagePath = packageName.replace('.', '/');

        // Assuming the test is run from project root, adjust path as needed
        Path srcPath = Paths.get("cwms-data-api/src/main/java", packagePath);

        if (!Files.exists(srcPath)) {
            // Try alternative path
            srcPath = Paths.get("src/main/java", packagePath);
        }

        try {
            scanDirectoryWithJavaParser(type, srcPath, packageName, handlerClasses);
        } catch (IOException e) {
            throw new RuntimeException("Error scanning for handler classes", e);
        }

        List<String> failures = new ArrayList<>();
        List<Class<T>> output = new ArrayList<>();

        for (String clazz : handlerClasses) {
            try {
                output.add((Class<T>) Class.forName(clazz));
            } catch (ClassNotFoundException ex) {
                failures.add(clazz);
            }
        }

        assertTrue(failures.isEmpty(), "Unable to find classes for " + String.join(", ", failures));

        return output;
    }

    private static <T> void scanDirectoryWithJavaParser(Class<T> type, Path directory, String packageName,
                                                    List<String> handlerClasses) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        Files.walk(directory, 1).forEach(path -> {
            if (Files.isDirectory(path) && !path.equals(directory)) {
                // Recursively scan subdirectories
                String subPackage = packageName + "." + path.getFileName().toString();
                try {
                    scanDirectoryWithJavaParser(type, path, subPackage, handlerClasses);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else if (path.toString().endsWith(".java")) {
                try {
                    analyzeJavaFile(path, packageName, handlerClasses);
                } catch (IOException e) {
                    System.err.println("Error parsing file: " + path);
                }
            }
        });
    }

    private static void analyzeJavaFile(Path javaFile, String packageName,
                                        List<String> handlerClasses) throws IOException {
        JavaParser parser = new JavaParser();
        ParseResult<CompilationUnit> parseResult = parser.parse(javaFile);

        if (!parseResult.isSuccessful()) {
            return;
        }

        CompilationUnit cu = parseResult.getResult().orElse(null);
        if (cu == null) {
            return;
        }

        // Find all class declarations
        cu.findAll(ClassOrInterfaceDeclaration.class).forEach(classDecl -> {
            // Skip interfaces and abstract classes
            if (classDecl.isInterface() || classDecl.isAbstract()) {
                return;
            }

            boolean isHandler = false;

            // Check implemented interfaces
            for (ClassOrInterfaceType implementedType : classDecl.getImplementedTypes()) {
                String typeName = implementedType.getNameAsString();
                String fullTypeName = implementedType.asString();

                if ("CrudHandler".equals(typeName) || "io.javalin.apibuilder.CrudHandler".equals(fullTypeName)) {
                    isHandler = true;
                    break;
                }

                if ("Handler".equals(typeName) || "io.javalin.http.Handler".equals(fullTypeName)) {
                    isHandler = true;
                    break;
                }
            }

            // Check extended classes (in case they extend a base class that implements the interface)
            for (ClassOrInterfaceType extendedType : classDecl.getExtendedTypes()) {
                String typeName = extendedType.getNameAsString();

                // Check if extends BaseCrudHandler or BaseHandler
                if ("BaseCrudHandler".equals(typeName) || "BaseHandler".equals(typeName)) {
                    isHandler = true;
                    break;
                }
            }

            if (isHandler) {
                String className = packageName + "." + classDecl.getNameAsString();
                handlerClasses.add(className);
            }
        });
    }

    public static class OpenApiDocInfo {
        Set<String> query = new LinkedHashSet<>();
        Set<String> path = new LinkedHashSet<>();
    }
}
