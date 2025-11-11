/*
 * MIT License
 * Copyright (c) 2025 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.expr.ClassExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.stmt.ThrowStmt;
import com.google.common.flogger.FluentLogger;
import helpers.OpenApiDocInfo;
import helpers.OpenApiDocTestInfo;
import helpers.OpenApiParamInfo;
import helpers.OpenApiParamUsage;
import helpers.OpenApiParamUsageInfo;
import helpers.OpenApiTestHelper;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Handler;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

class OpenApiDocTest {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    @MethodSource(value = "getHandlerDocInfo")
    @ParameterizedTest
    void test_handler_documentation(OpenApiDocTestInfo testInfo) throws IOException {
        CompilationUnit compilationUnit = OpenApiTestHelper.readCompilationUnit(testInfo.getClazz());
        assertAll(buildTestAssertions(compilationUnit, testInfo));
    }

    @MethodSource(value = "getCrudHandlerDocInfo")
    @ParameterizedTest
    void test_crud_handler_documentation(OpenApiDocTestInfo testInfo) throws IOException {
        CompilationUnit compilationUnit = OpenApiTestHelper.readCompilationUnit(testInfo.getClazz());
        assertAll(buildTestAssertions(compilationUnit, testInfo));
    }

    @Test
    void test_time_series_controller() throws IOException {
        OpenApiDocTestInfo testInfo = OpenApiTestHelper.readOpenApiDocs(CrudHandler.class, TimeSeriesController.class);
        CompilationUnit compilationUnit = OpenApiTestHelper.readCompilationUnit(testInfo.getClazz());
        assertAll(buildTestAssertions(compilationUnit, testInfo));
    }

    private Stream<Executable> buildTestAssertions(CompilationUnit compilationUnit, OpenApiDocTestInfo testInfo) {
        return testInfo.getMethodDocs()
                       .stream()
                       .map(docInfo -> validateOpenApiDoc(compilationUnit, docInfo, testInfo.getClazz()));
    }

    private Executable validateOpenApiDoc(CompilationUnit unit, OpenApiDocInfo testInfo, Class<?> clazz){
        Executable output;
        if (testInfo.isIgnored()) {
            output = testIgnoredMethod(unit, testInfo, clazz);
        } else {
            OpenApiParamUsage parsedParamInfo = parseParamInfo(unit, clazz, testInfo.getMethod());
            output = testMethod(testInfo, parsedParamInfo);
        }
        return output;
    }

    private Executable testIgnoredMethod(CompilationUnit unit, OpenApiDocInfo testInfo, Class<?> clazz) {

        // 16/24 cases throw an UnsupportedOperationException, so verify that behavior here for now.
        // Other cases use the context to provide additional feedback, but the feedback is not consistent.
        // Could be an improvement for the corps to define it more strictly here.
        MethodDeclaration method = getMethodDeclaration(unit, testInfo.getMethod());
        boolean throwsException = method.findAll(ThrowStmt.class)
                                        .stream()
                                        .anyMatch(throwStmt -> {
                                            if (throwStmt.getExpression() instanceof ObjectCreationExpr) {
                                                ObjectCreationExpr creation = (ObjectCreationExpr) throwStmt.getExpression();
                                                return creation.getType().getNameAsString().equals("UnsupportedOperationException");
                                            }
                                            return false;
                                        });
        return () -> assertTrue(throwsException, clazz.getSimpleName() + "::" + testInfo.getMethod().getName() + " is marked as ignored, but does not throw an UnsupportedOperationException.");
    }

    private Executable testMethod(OpenApiDocInfo testInfo,
                                  OpenApiParamUsage parsedParamInfo) {
        List<OpenApiParamInfo> expectedQueryParameters = testInfo.getQueryParameters();
        List<OpenApiParamInfo> expectedPathParameters = testInfo.getPathParameters();

        Set<OpenApiParamUsageInfo> receivedQueryParameters = parsedParamInfo.getQueryParams();
        Set<OpenApiParamUsageInfo> receivedPathParameters = parsedParamInfo.getPathParams();
        OpenApiParamUsageInfo receivedResourceId = parsedParamInfo.getResourceId();
        return () -> assertAll("Testing " + testInfo.getMethod().getName(),
                               () -> testQueryParameters(expectedQueryParameters, receivedQueryParameters),
                               () -> testPathParameters(expectedPathParameters, receivedPathParameters, receivedResourceId));
    }

    private void testPathParameters(List<OpenApiParamInfo> expectedPathParameters, Set<OpenApiParamUsageInfo> receivedPathParameters,
                                    OpenApiParamUsageInfo receivedResourceId) {
        List<OpenApiParamUsageInfo> verifiedUsages = new ArrayList<>();
        List<OpenApiParamInfo> expectedParams = new ArrayList<>();
        List<OpenApiParamInfo> missingItems = new ArrayList<>();
        List<OpenApiParamUsageInfo> receivedItems = new ArrayList<>(receivedPathParameters);

        if (receivedResourceId != null) {
            //Special case, equivalent to the last expectedPathParameters, but it can have an ambiguous name.
            if (!expectedPathParameters.isEmpty()) {
                String name = expectedPathParameters.get(expectedPathParameters.size() - 1).getName();
                receivedResourceId.getParamInfo().setName(name);
            }

            receivedItems.add(receivedResourceId);
        }

        for (OpenApiParamInfo paramInfo : expectedPathParameters) {
            OpenApiParamUsageInfo equivalent = null;
            for (OpenApiParamUsageInfo paramUsage : receivedItems) {
                if (paramUsage.getParamInfo().getName().equals(paramInfo.getName())) {
                    equivalent = paramUsage;
                    break;
                }
            }
            if (equivalent == null) {
                missingItems.add(paramInfo);
            } else {
                receivedItems.remove(equivalent);
                verifiedUsages.add(equivalent);
                expectedParams.add(paramInfo);
            }
        }

        String extraInfo = receivedItems.stream()
                                        .map(p -> p.getParamInfo().getName())
                                        .collect(Collectors.joining(", "));
        String missingInfo = missingItems.stream()
                                         .map(OpenApiParamInfo::getName)
                                         .collect(Collectors.joining(", "));
        assertAll(() -> assertTrue(receivedItems.isEmpty(), "Found extra path parameters: " + extraInfo),
                  () -> assertTrue(missingItems.isEmpty(), "Found missing path parameters: " + missingInfo),
                  () -> assertAll(expectedParams.stream().map(expectedParam -> testParamInfo(expectedParam, verifiedUsages))));
    }

    private void testQueryParameters(List<OpenApiParamInfo> expectedQueryParameters,
                                     Set<OpenApiParamUsageInfo> receivedQueryParameters) {
        List<OpenApiParamUsageInfo> verifiedUsages = new ArrayList<>();
        List<OpenApiParamInfo> expectedParams = new ArrayList<>();
        List<OpenApiParamInfo> missingItems = new ArrayList<>();
        List<OpenApiParamUsageInfo> receivedItems = new ArrayList<>(receivedQueryParameters);

        for (OpenApiParamInfo paramInfo : expectedQueryParameters) {
            OpenApiParamUsageInfo equivalent = null;
            for (OpenApiParamUsageInfo paramUsage : receivedItems) {
                if (paramUsage.getParamInfo().getName().equals(paramInfo.getName())) {
                    equivalent = paramUsage;
                    break;
                }
            }
            if (equivalent == null) {
                missingItems.add(paramInfo);
            } else {
                receivedItems.remove(equivalent);
                verifiedUsages.add(equivalent);
                expectedParams.add(paramInfo);
            }
        }

        String extraInfo = receivedItems.stream()
                                          .map(p -> p.getParamInfo().getName())
                                          .collect(Collectors.joining(", "));
        String missingInfo = missingItems.stream()
                                         .map(OpenApiParamInfo::getName)
                                         .collect(Collectors.joining(", "));
        assertAll(() -> assertTrue(receivedItems.isEmpty(), "Found extra query parameters: " + extraInfo),
                  () -> assertTrue(missingItems.isEmpty(), "Found missing query parameters: " + missingInfo),
                  () -> assertAll(expectedParams.stream().map(expectedParam -> testParamInfo(expectedParam, verifiedUsages))));
    }

    private Executable testParamInfo(OpenApiParamInfo expectedParam,
                                     List<OpenApiParamUsageInfo> receivedQueryParameters) {
        OpenApiParamUsageInfo receivedInfo = receivedQueryParameters.stream()
                                                                    .filter(receivedUsageInfo -> receivedUsageInfo.getParamInfo()
                                                                                                                                       .getName()
                                                                                                                                       .equals(expectedParam.getName()))
                                                                    .findFirst()
                                                                    .orElse(null);
        assertNotNull(receivedInfo, "Unable to find " + expectedParam.getName() + " in the code.");
        return () -> assertAll(() -> assertTrue(receivedInfo.isUsed(), "Unable to find a usage of " + expectedParam.getName()),
                               () -> assertTrue(receivedInfo.isNullHandled(), "Unable to find a null handled usage of " + expectedParam.getName()));
    }

    private OpenApiParamUsage parseParamInfo(CompilationUnit unit, Class<?> clazz, Method method) {
        MethodDeclaration methodDeclaration = getMethodDeclaration(unit, method);
        String context = methodDeclaration.getParameter(0).getNameAsString();

        List<MethodCallExpr> methodCalls = methodDeclaration.findAll(MethodCallExpr.class);
        List<OpenApiParamUsageInfo> optionalTypedQueryParams = readParamUsagesFromCall(methodCalls, call -> readQueryParamAsClassFromCall(unit, context, clazz, call), "queryParamAsClass");

        List<OpenApiParamUsageInfo> optionalStringQueryParams = methodCalls.stream()
                                                                           .filter(call -> call.getNameAsString().equals("queryParam"))
                                                                           .map(call -> readUsageFromCall(unit, clazz, call, false))
                                                                           .collect(Collectors.toList());

        List<OpenApiParamUsageInfo> requiredQueryParams = methodCalls.stream()
                                                                     .filter(call -> call.getNameAsString().equals("requiredParam"))
                                                                     .map(call -> readUsageFromCall(unit, clazz, call, true))
                                                                     .collect(Collectors.toList());

        List<OpenApiParamUsageInfo> optionalTimeQueryParams = methodCalls.stream()
                                                                         .filter(call -> call.getNameAsString().equals("queryParamAsInstant") ||
                                                                                 call.getNameAsString().equals("queryParamAsZdt"))
                                                                         .map(call -> readJavaTimeFromCall(call, false))
                                                                         .flatMap(List::stream)
                                                                         .collect(Collectors.toList());

        List<OpenApiParamUsageInfo> requiredTimeQueryParams = methodCalls.stream()
                                                                         .filter(call -> call.getNameAsString().equals("requiredZdt") ||
                                                                                 call.getNameAsString().equals("requiredInstant"))
                                                                         .map(call -> readJavaTimeFromCall(call, true))
                                                                         .flatMap(List::stream)
                                                                         .collect(Collectors.toList());

        Set<OpenApiParamUsageInfo> queryParams = new HashSet<>(optionalStringQueryParams);
        queryParams.addAll(optionalTypedQueryParams);
        queryParams.addAll(requiredQueryParams);
        queryParams.addAll(optionalTimeQueryParams);
        queryParams.addAll(requiredTimeQueryParams);


        Set<OpenApiParamUsageInfo> pathParams = methodCalls.stream()
                                                     .filter(call -> call.getNameAsString().equals("pathParam"))
                                                     .map(call -> readUsageFromCall(unit, clazz, call, true))
                                                     .collect(Collectors.toSet());

        OpenApiParamUsageInfo resourceId = null;

        if (methodDeclaration.getParameters().size() > 1) {
            Parameter param = methodDeclaration.getParameter(1);
            String paramId = param.getNameAsString();
            OpenApiParamInfo paramInfo = new OpenApiParamInfo(paramId, true, String.class);
            boolean isUsed = methodDeclaration.findAll(MethodCallExpr.class)
                                              .stream()
                                              .anyMatch(call -> call.toString().contains(paramId));
            resourceId = new OpenApiParamUsageInfo(paramInfo, isUsed, true);
        }

        return new OpenApiParamUsage(pathParams, queryParams, resourceId);
    }

    private OpenApiParamUsageInfo readQueryParamAsClassFromCall(CompilationUnit unit, String context, Class<?> clazz, MethodCallExpr call) {
        return call.getScope()
                   .map(scope -> {
                       if (scope.isNameExpr()) {
                           return readQueryParamAsClassFromContextCall(unit, clazz, call);
                       } else {
                           return readQueryParamAsClassFromControllersCall(unit, clazz, call);
                       }
                   }).orElseGet(() -> readQueryParamAsClassFromControllersCall(unit, clazz, call));
    }

    private OpenApiParamUsageInfo readQueryParamAsClassFromContextCall(CompilationUnit unit, Class<?> clazz, MethodCallExpr call) {
        // First argument is the parameter name (usually a string literal or constant)
        String paramName = parseParameterName(call.getArgument(0));

        Class<?> paramClass = String.class;
        if (call.getArguments().size() > 1) {
            // Second argument is the class (e.g., Boolean.class, String.class)
            ClassExpr argument = call.getArgument(1).asClassExpr();
            paramClass = identifyClassFromExpression(unit, clazz, argument);
        }
        boolean used = true;
        boolean nullHandled = true;
        return new OpenApiParamUsageInfo(new OpenApiParamInfo(paramName, false, paramClass), used, nullHandled);
    }

    private OpenApiParamUsageInfo readQueryParamAsClassFromControllersCall(CompilationUnit unit, Class<?> clazz, MethodCallExpr call) {
        Expression arg1 = call.getArgument(1);
        Class<?> type;
        String name;
        if (arg1.isArrayCreationExpr()) {
            //Context, String[], Class, T, {metrics}, {className}
            type = identifyClassFromExpression(unit, clazz, call.getArgument(2).asClassExpr());
            name = parseParameterName(arg1.asArrayCreationExpr().getInitializer().orElse(null).getValues().get(0));
        } else if (arg1.isClassExpr()) {
           //Context, Class, T, Name, [Aliases]
            type = identifyClassFromExpression(unit, clazz, arg1.asClassExpr());
            name = parseParameterName(call.getArgument(3));
        } else {
            //Unknown case for queryParamAsClass (new method to handle?
            throw new UnsupportedOperationException("Unsupported argument[1] type for queryParamAsClass: " + arg1.getClass());
        }

        return new OpenApiParamUsageInfo(new OpenApiParamInfo(name, false, type), true, true);
    }

    private List<OpenApiParamUsageInfo> readParamUsagesFromCall(List<MethodCallExpr> methodCalls,
                                                                Function<MethodCallExpr, OpenApiParamUsageInfo> paramReader,
                                                                String... functions) {
        List<String> realFunctions = Arrays.asList(functions);
        return methodCalls.stream()
                          .filter(call -> realFunctions.contains(call.getNameAsString()))
                          .map(paramReader)
                          .collect(Collectors.toList());
    }

    private List<OpenApiParamUsageInfo> readJavaTimeFromCall(MethodCallExpr call, boolean required) {
        //Should only be 2 parameters, and parameter 2 is the parameter name
        String paramName = parseParameterName(call.getArgument(1));
        Class<?> type = String.class;
        boolean used = true;
        boolean nullHandled = true;
        if (!required) {
            //Check if null is handled via getOrDefault
        }
        return Arrays.asList(new OpenApiParamUsageInfo(new OpenApiParamInfo(paramName, required, type), used, nullHandled),
                             new OpenApiParamUsageInfo(new OpenApiParamInfo(Controllers.TIMEZONE, required, type), used, nullHandled));
    }

    private OpenApiParamUsageInfo readUsageFromCall(CompilationUnit unit, Class<?> clazz, MethodCallExpr call, boolean required) {
        //We have a scope, so it's called from something like context.
        return call.getScope().map(exp -> {
            // First argument is the parameter name (usually a string literal or constant)
            String paramName = parseParameterName(call.getArgument(0));

            Class<?> paramClass = String.class;
            if (call.getArguments().size() > 1) {
                // Second argument is the class (e.g., Boolean.class, String.class)
                ClassExpr argument = call.getArgument(1).asClassExpr();
                paramClass = identifyClassFromExpression(unit, clazz, argument);
            }
            boolean used = true;
            boolean nullHandled = true;
            if (!required) {
                //Check if null is handled via getOrDefault
            }
            return new OpenApiParamUsageInfo(new OpenApiParamInfo(paramName, required, paramClass), used, nullHandled);
        }).orElseGet(() -> {
            //It's calling a function, so most likely argument 0 is context, argument 1 is the identifier, and argument 2 is class.
            String paramName = parseParameterName(call.getArgument(1));
            Class<?> paramClass = String.class;
            if (call.getArguments().size() > 2) {
                ClassExpr argument = call.getArgument(2).asClassExpr();
                paramClass = identifyClassFromExpression(unit, clazz, argument);
            } else if (call.getNameAsString().endsWith("AsDouble")) {
                paramClass = Double.class;
            } else if (call.getNameAsString().endsWith("AsString")) {
                paramClass = String.class;
            }
            boolean nullHandled = true;
            return new OpenApiParamUsageInfo(new OpenApiParamInfo(paramName, required, paramClass), true, nullHandled);
        });
    }

    private static @NotNull String parseParameterName(Expression arg) {
        String value;
        if (arg instanceof StringLiteralExpr) {
            // Using a literal, which is problematic on its own, but we can get the value.
            value = arg.asStringLiteralExpr().getValue();
        } else if (arg instanceof FieldAccessExpr) {
            // It's using a field accessor - this means it's calling Controllers.NAME for instance.
            // This doesn't apply to when the field is statically imported though.
            FieldAccessExpr fieldExp = arg.asFieldAccessExpr();
            value = parseParameterName(fieldExp.getNameAsExpression());
        } else if (arg instanceof NameExpr) {
            // Assume it's coming from the Controllers class, as that's a normal location for constants
            // This is effectively like...we have a name, but we're not sure where it's coming from and we're
            // Not a string literal.
            try {
                Field field = Controllers.class.getField(arg.asNameExpr().getNameAsString());
                value = field.get(null).toString();
            }  catch (Exception e) {
                throw new UnsupportedOperationException("Unable to find Controllers field for " + arg.asNameExpr().getNameAsString(), e);
            }
        } else {
            value = arg.toString();
        }
        return value;
    }

    private Class<?> identifyClassFromExpression(CompilationUnit unit, Class<?> clazz, ClassExpr expression) {
        // This may or may not give us the fully qualified class name (depends on if the code uses that)
        String className = expression.getTypeAsString();
        try {
            return Class.forName(className);
        }  catch (ClassNotFoundException e) {
            LOGGER.atFinest().withCause(e).log("Ignored, checking more imports.");
        }

        // Check java.lang classes, since they don't need imports.
        try {
            return Class.forName("java.lang." + className);
        } catch (ClassNotFoundException e) {
            LOGGER.atFinest().withCause(e).log("Ignored, checking more imports.");
        }

        // Check current package, since it doesn't need imports either
        try {
            return Class.forName(clazz.getPackageName() + "." + className);
        } catch (ClassNotFoundException e) {
            LOGGER.atFinest().withCause(e).log("Ignored, checking more imports.");
        }

        Class<?> output = null;

        for (ImportDeclaration importDeclaration : unit.getImports()) {
            String name = importDeclaration.getNameAsString();
            if (name.endsWith("." + className)) {
                try {
                    output = Class.forName(name);
                    break;
                } catch (ClassNotFoundException e) {
                    //Not sure how this happened...seems bad
                    LOGGER.atSevere().withCause(e).log("Unable to find class for name " + name + ".");
                }
            }

            if (importDeclaration.isAsterisk()) {
                try {
                    output = Class.forName(name + className);
                    break;
                } catch (ClassNotFoundException e) {
                    LOGGER.atFinest().withCause(e).log("Ignored, checking more imports.");
                }
            }
        }

        return output;
    }

    private static MethodDeclaration getMethodDeclaration(CompilationUnit compilationUnit, Method method) {
        return compilationUnit.findAll(MethodDeclaration.class)
                              .stream()
                              .filter(m -> m.getNameAsString().equals(method.getName()))
                              .filter(m -> m.getParameters().size() == method.getParameterCount())
                              .findFirst()
                              .orElseThrow(() -> new AssertionError("Method " + method.getName() + " not found"));
    }

    static Stream<OpenApiDocTestInfo> getHandlerDocInfo() {
        List<Class<Handler>> handlers = OpenApiTestHelper.findClassesOfType(Handler.class);
        return handlers.stream()
                       .map(clazz -> OpenApiTestHelper.readOpenApiDocs(Handler.class, clazz));
    }

    static Stream<OpenApiDocTestInfo> getCrudHandlerDocInfo() {
        List<Class<CrudHandler>> handlers = OpenApiTestHelper.findClassesOfType(CrudHandler.class);
        return handlers.stream()
                       .map(clazz -> OpenApiTestHelper.readOpenApiDocs(CrudHandler.class, clazz));
    }
}
