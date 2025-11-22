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
import com.github.javaparser.resolution.Resolvable;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.github.javaparser.resolution.declarations.ResolvedValueDeclaration;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
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
        OpenApiDocTestInfo testInfo = OpenApiTestHelper.readOpenApiDocs(CrudHandler.class, StateController.class);
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

        // Expected format for ignored methods is just one call to:
        // `ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());`
        MethodDeclaration method = getMethodDeclaration(unit, testInfo.getMethod());

        Optional<MethodCallExpr> statusCall = method.findAll(MethodCallExpr.class)
                                                    .stream()
                                                    .filter(exp -> exp.getNameAsString().equals("status"))
                                                    .findFirst();

        Optional<MethodCallExpr> jsonCall = method.findAll(MethodCallExpr.class)
                                                  .stream()
                                                  .filter(exp -> exp.getNameAsString().equals("json"))
                                                  .findFirst();

        try {
            boolean usesStatus = statusCall.isPresent();
            boolean isCorrectCode = statusCall.stream()
                                              .map(exp -> parseParameterName(exp.getArgument(0)))
                                              .mapToInt(Integer::parseInt)
                                              .anyMatch(v -> v == HttpServletResponse.SC_NOT_IMPLEMENTED);

            boolean usesJson = jsonCall.isPresent();
            boolean isCorrectJson = jsonCall.map(exp -> parseParameterName(exp.getArgument(0)))
                                            .map("CdaError.notImplemented()"::equals)
                                            .orElse(false);
            return () -> assertAll(
                    "Testing ignored method " + method.getNameAsString() + ":  Incorrect response for ignored endpoint.  Expecting `ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented())`",
                    () -> assertTrue(usesStatus && isCorrectCode,
                                     "Incorrect status code used, context should provide HttpServletResponse.SC_NOT_IMPLEMENTED."),
                    () -> assertTrue(usesJson && isCorrectJson,
                                     "Incorrect JSON returned, context should respond with CdaError.notImplemented()"));
        } catch (Exception ex) {
            return () -> fail("Testing ignored method " + method.getNameAsString() + ":  Error analyzing method.  Expected `ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());`.", ex);
        }
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
        Set<OpenApiParamUsageInfo> verifiedUsages = new HashSet<>();
        Set<OpenApiParamInfo> expectedParams = new HashSet<>();
        Set<OpenApiParamInfo> missingItems = new HashSet<>();
        Set<OpenApiParamUsageInfo> receivedItems = new HashSet<>(receivedPathParameters);

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
        assertAll(() -> assertTrue(receivedItems.isEmpty(), "Found used undocumented path parameter: " + extraInfo),
                  () -> assertTrue(missingItems.isEmpty(), "Found documented path parameter that is not used: " + missingInfo),
                  () -> assertAll(expectedParams.stream().map(expectedParam -> testParamInfo(expectedParam, verifiedUsages))));
    }

    private void testQueryParameters(List<OpenApiParamInfo> expectedQueryParameters,
                                     Set<OpenApiParamUsageInfo> receivedQueryParameters) {
        Set<OpenApiParamUsageInfo> verifiedUsages = new HashSet<>();
        Set<OpenApiParamInfo> expectedParams = new HashSet<>();
        Set<OpenApiParamInfo> missingItems = new HashSet<>();
        Set<OpenApiParamUsageInfo> receivedItems = new HashSet<>(receivedQueryParameters);

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
        assertAll(() -> assertTrue(receivedItems.isEmpty(), "Found used undocumented query parameter: " + extraInfo),
                  () -> assertTrue(missingItems.isEmpty(), "Found documented query parameter that is not used: " + missingInfo),
                  () -> assertAll(expectedParams.stream().map(expectedParam -> testParamInfo(expectedParam, verifiedUsages))));
    }

    private Executable testParamInfo(OpenApiParamInfo expectedParam,
                                     Set<OpenApiParamUsageInfo> receivedQueryParameters) {
        OpenApiParamUsageInfo receivedInfo = receivedQueryParameters.stream()
                                                                    .filter(receivedUsageInfo -> receivedUsageInfo.getParamInfo()
                                                                                                                                       .getName()
                                                                                                                                       .equals(expectedParam.getName()))
                                                                    .findFirst()
                                                                    .orElse(null);

        //This should not happen, just a sanity check
        assertNotNull(receivedInfo, "Unable to find " + expectedParam.getName() + " in the code.");

        //Real tests
        return () -> assertAll(() -> assertTrue(receivedInfo.isUsed(), "Unable to find a usage of documented parameter: " + expectedParam.getName()),
                               () -> assertTrue(receivedInfo.isNullHandled(), "Unable to find a null handled usage of documented parameter: " + expectedParam.getName()));
    }

    private OpenApiParamUsage parseParamInfo(CompilationUnit unit, Class<?> clazz, Method method) {
        MethodDeclaration methodDeclaration = getMethodDeclaration(unit, method);
        String context = methodDeclaration.getParameter(0).getNameAsString();

        List<MethodCallExpr> methodCalls = methodDeclaration.findAll(MethodCallExpr.class);
        Set<OpenApiParamUsageInfo> optionalTypedQueryParams = readParamUsagesFromCall(methodCalls, call -> readQueryParamAsClassFromCall(unit, context, clazz, call), "queryParamAsClass");
        Set<OpenApiParamUsageInfo> optionalDoubleQueryParams = readParamUsagesFromCall(methodCalls, call -> readUsageFromCall(unit, clazz, call, false), "queryParamAsDouble");

        Set<OpenApiParamUsageInfo> optionalStringQueryParams = methodCalls.stream()
                                                                           .filter(call -> call.getNameAsString().equals("queryParam"))
                                                                           .map(call -> readUsageFromCall(unit, clazz, call, false))
                                                                           .collect(Collectors.toSet());

        Set<OpenApiParamUsageInfo> requiredQueryParams = methodCalls.stream()
                                                                     .filter(call -> call.getNameAsString().equals("requiredParam") ||
                                                                             call.getNameAsString().equals("requiredParamAs"))
                                                                     .map(call -> readUsageFromCall(unit, clazz, call, true))
                                                                     .collect(Collectors.toSet());

        Set<OpenApiParamUsageInfo> optionalTimeQueryParams = methodCalls.stream()
                                                                         .filter(call -> call.getNameAsString().equals("queryParamAsInstant") ||
                                                                                 call.getNameAsString().equals("queryParamAsZdt"))
                                                                         .map(call -> readJavaTimeFromCall(call, false))
                                                                         .flatMap(Set::stream)
                                                                         .collect(Collectors.toSet());

        Set<OpenApiParamUsageInfo> requiredTimeQueryParams = methodCalls.stream()
                                                                         .filter(call -> call.getNameAsString().equals("requiredZdt") ||
                                                                                 call.getNameAsString().equals("requiredInstant"))
                                                                         .map(call -> readJavaTimeFromCall(call, true))
                                                                         .flatMap(Set::stream)
                                                                         .collect(Collectors.toSet());

        Set<OpenApiParamUsageInfo> queryParams = new HashSet<>(optionalStringQueryParams);
        queryParams.addAll(optionalTypedQueryParams);
        queryParams.addAll(requiredQueryParams);
        queryParams.addAll(optionalTimeQueryParams);
        queryParams.addAll(requiredTimeQueryParams);
        queryParams.addAll(optionalDoubleQueryParams);


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

    private Set<OpenApiParamUsageInfo> readParamUsagesFromCall(List<MethodCallExpr> methodCalls,
                                                                Function<MethodCallExpr, OpenApiParamUsageInfo> paramReader,
                                                                String... functions) {
        List<String> realFunctions = Arrays.asList(functions);
        return methodCalls.stream()
                          .filter(call -> realFunctions.contains(call.getNameAsString()))
                          .map(paramReader)
                          .collect(Collectors.toSet());
    }

    private Set<OpenApiParamUsageInfo> readJavaTimeFromCall(MethodCallExpr call, boolean required) {
        //Should only be 2 parameters, and parameter 2 is the parameter name
        String paramName = parseParameterName(call.getArgument(1));
        Class<?> type = String.class;
        boolean used = true;
        boolean nullHandled = true;
        return Set.of(new OpenApiParamUsageInfo(new OpenApiParamInfo(paramName, required, type), used, nullHandled),
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

    private @NotNull String parseParameterName(Expression arg) {
        String value;
        if (arg.isStringLiteralExpr()) {
            // Using a literal, which is problematic on its own, but we can get the value.
            value = arg.asStringLiteralExpr().getValue();
        } else if (arg.isFieldAccessExpr()) {
            // It's using a field accessor - this means it's calling Controllers.NAME for instance.
            // This doesn't apply to when the field is statically imported though.
            FieldAccessExpr exp = arg.asFieldAccessExpr();
            value = resolveValue(exp, exp.getNameAsString());
        } else if (arg.isNameExpr()) {
            NameExpr exp = arg.asNameExpr();
            value = resolveValue(exp, exp.getNameAsString());
        } else {
            value = arg.toString();
        }
        return value;
    }

    private String resolveValue(Resolvable<ResolvedValueDeclaration> exp, String name) {
        try {
            ResolvedValueDeclaration resolve = exp.resolve();
            if (resolve.isField()) {
                Class<?> clazz = Class.forName(resolve.asField().declaringType().getQualifiedName());
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(null).toString();
            } else if (resolve.isEnumConstant()) {
                return resolve.asEnumConstant().getName();
            } else {
                throw new UnsupportedOperationException("Unable to parse resolved value declaration type of " + resolve.getClass().getName());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
