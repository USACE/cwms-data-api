package helpers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

public class OpenApiTestHelper {



    public static List<Method> findByName(Class<?> klass, String name){
        Method[] methods = klass.getMethods();
        return Stream.of(methods).filter(m -> m.getName().equals(name)).collect(Collectors.toList());
    }

    public static Method findOneByName(Class<?> klass, String name){
        List<Method> methods = findByName(klass, name);
        if(methods == null || methods.isEmpty()) throw new RuntimeException("Did not find method with name " + name);
        if(methods.size() > 1) throw new RuntimeException("Multiple methods with name " + name);
        return methods.get(0);
    }


    static class OpenApiDocInfo {
        Set<String> query = new LinkedHashSet<>();
        Set<String> path = new LinkedHashSet<>();
    }

    public static OpenApiDocInfo readDocParams(Method m) {
        OpenApiDocInfo info = new OpenApiDocInfo();
        io.javalin.plugin.openapi.annotations.OpenApi oa = m.getAnnotation(io.javalin.plugin.openapi.annotations.OpenApi.class);
        if (oa == null || oa.ignore()) return info;
        for (io.javalin.plugin.openapi.annotations.OpenApiParam p : oa.queryParams()) {
            if (p != null && !p.name().isBlank()) info.query.add(p.name());
        }
        for (io.javalin.plugin.openapi.annotations.OpenApiParam p : oa.pathParams()) {
            if (p != null && !p.name().isBlank()) info.path.add(p.name());
        }
        return info;
    }
    
    public static List<String> findCrudHandlerClasses() {
        return findHandlerClassNames("cwms.cda.api");
    }

    public static List<String> findHandlerClassNames(String packageName) {
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
            scanDirectoryWithJavaParser(srcPath, packageName, handlerClasses);
        } catch (IOException e) {
            throw new RuntimeException("Error scanning for handler classes", e);
        }

        return handlerClasses;
    }

    private static void scanDirectoryWithJavaParser(Path directory, String packageName,
                                                    List<String> handlerClasses) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        Files.walk(directory, 1).forEach(path -> {
            if (Files.isDirectory(path) && !path.equals(directory)) {
                // Recursively scan subdirectories
                String subPackage = packageName + "." + path.getFileName().toString();
                try {
                    scanDirectoryWithJavaParser(path, subPackage, handlerClasses);
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
    
}
