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
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.stmt.ThrowStmt;
import helpers.OpenApiDocInfo;
import helpers.OpenApiDocTestInfo;
import helpers.OpenApiParamInfo;
import helpers.OpenApiParamUsageInfo;
import helpers.OpenApiTestHelper;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Handler;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

class OpenApiDocTest {

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
            List<OpenApiParamUsageInfo> parsedParamInfo = parseParamInfo(unit, testInfo.getMethod());
            output = testMethod(unit, testInfo, parsedParamInfo);
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

    private Executable testMethod(CompilationUnit unit, OpenApiDocInfo testInfo,
                                  List<OpenApiParamUsageInfo> parsedParamInfo) {
        return () -> assertTrue(true);
    }

    private List<OpenApiParamUsageInfo> parseParamInfo(CompilationUnit compilationUnit, Method method) {
        List<OpenApiParamUsageInfo> output = new ArrayList<>();
        MethodDeclaration methodDeclaration = getMethodDeclaration(compilationUnit, method);

        List<MethodCallExpr> methodCalls = methodDeclaration.findAll(MethodCallExpr.class);
        List<MethodCallExpr> optionalTypedQueryParams = methodCalls.stream()
                                                                   .filter(call -> call.getNameAsString().equals("queryParamAsClass"))
                                                                   .collect(Collectors.toList());

        List<MethodCallExpr> optionalStringQueryParams = methodCalls.stream()
                                                                    .filter(call -> call.getNameAsString().equals("queryParam"))
                                                                    .collect(Collectors.toList());

        List<MethodCallExpr> requiredQueryParams = methodCalls.stream()
                                                              .filter(call -> call.getNameAsString().equals("requiredParam"))
                                                              .collect(Collectors.toList());

        List<MethodCallExpr> pathParams = methodCalls.stream()
                                                     .filter(call -> call.getNameAsString().equals("pathParam"))
                                                     .collect(Collectors.toList());

        if (methodDeclaration.getParameters().size() > 1)
        {
            Parameter param = methodDeclaration.getParameter(1);
            //Resource id parameter
            String paramId = param.getNameAsString();
            OpenApiParamInfo paramInfo = new OpenApiParamInfo(paramId, true, String.class);
            boolean isUsed = methodDeclaration.findAll(MethodCallExpr.class)
                                              .stream()
                                              .anyMatch(call -> call.toString().contains(paramId));
            output.add(new OpenApiParamUsageInfo(paramInfo, isUsed));
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
