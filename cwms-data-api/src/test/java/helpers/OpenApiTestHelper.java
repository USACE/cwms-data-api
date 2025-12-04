package helpers;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ClassLoaderTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
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

    public static OpenApiDocInfo readDocParams(Method m) {
        OpenApi oa = m.getAnnotation(OpenApi.class);
        if (oa == null || oa.ignore()) {
            return new OpenApiDocInfo(m, true);
        }
        OpenApiDocInfo info = new OpenApiDocInfo(m, false);
        for (OpenApiParam p : oa.queryParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                OpenApiParamInfo paramObj = new OpenApiParamInfo(p.name(), p.required(), p.type());
                info.getQueryParameters().add(paramObj);
            }
        }
        for (OpenApiParam p : oa.pathParams()) {
            if (p != null && !p.name().trim().isEmpty()) {
                OpenApiParamInfo paramObj = new OpenApiParamInfo(p.name(), p.required(), p.type());
                info.getPathParameters().add(paramObj);
            }
        }
        return info;
    }

    private static String getFileNameWithoutExtension(Path path) {
        String temp = path.toString().replace("\\", "/");
        String fileName = temp.replace("cwms-data-api/src/main/java/", "")
                              .replace("src/main/java/", "")
                              .replace("/", ".");
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

        Path srcPath = getPackagePath(packagePath);

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

    private static Path getSrcRootPath() {
        Path srcPath = Paths.get("cwms-data-api/src/main/java");

        if (!Files.exists(srcPath)) {
            // Try alternative path
            srcPath = Paths.get("src/main/java");
        }

        return srcPath;
    }

    private static Path getPackagePath(String packagePath) {
        // Assuming the test is run from project root, adjust path as needed
        return getSrcRootPath().resolve(packagePath);
    }

    public static <T> OpenApiDocTestInfo readOpenApiDocs(Class<? super T> baseClass, Class<T> primaryClass) {

        List<OpenApiDocInfo> infoObjs = Arrays.stream(baseClass.getDeclaredMethods())
                                                     .map(method -> findOneByName(primaryClass, method.getName()))
                                                     .map(OpenApiTestHelper::readDocParams)
                                                     .collect(toList());
        return new OpenApiDocTestInfo(primaryClass, infoObjs);
    }

    public static CompilationUnit readCompilationUnit(Class<?> clazz) throws IOException {
        String fullyQualifiedName = clazz.getName().replace(".", "/") + ".java";
        Path path = getPackagePath(fullyQualifiedName);
        assertTrue(Files.exists(path));
        ParserConfiguration config = buildParserConfig();
        JavaParser parser = new JavaParser(config);
        return parser.parse(path)
                     .getResult()
                     .orElseThrow(() -> new RuntimeException("Failed to parse file"));
    }

    private static ParserConfiguration buildParserConfig() {
        CombinedTypeSolver combinedTypeSolver = new CombinedTypeSolver(new ReflectionTypeSolver(),
                                                                       new ClassLoaderTypeSolver(OpenApiTestHelper.class.getClassLoader()),
                                                                       new JavaParserTypeSolver(getSrcRootPath()));

        return new ParserConfiguration()
                .setSymbolResolver(new JavaSymbolSolver(combinedTypeSolver));
    }
}
