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
import com.github.javaparser.ast.expr.MethodCallExpr;
import helpers.OpenApiDocInfo;
import helpers.OpenApiDocTestInfo;
import helpers.OpenApiParamInfo;
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
import static org.junit.jupiter.api.Assertions.assertAll;

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
                       .map(docInfo -> () -> validateOpenApiDoc(compilationUnit, docInfo));
    }

    private void validateOpenApiDoc(CompilationUnit unit, OpenApiDocInfo testInfo) throws Exception {
        List<OpenApiParamInfo> parsedParamInfo = parseParamInfo(unit, testInfo.getMethod());
    }

    private List<OpenApiParamInfo> parseParamInfo(CompilationUnit compilationUnit, Method method) throws IOException {
        MethodDeclaration methodDeclaration = compilationUnit.findAll(MethodDeclaration.class)
                                                             .stream()
                                                             .filter(m -> m.getNameAsString().equals(method.getName()))
                                                             .filter(m -> m.getParameters().size() == method.getParameterCount()) //Not detailed enough, need to check types
                                                             .findFirst()
                                                             .orElseThrow(() -> new AssertionError("Method " + method.getName() + " not found"));

        List<MethodCallExpr> methodCalls = methodDeclaration.findAll(MethodCallExpr.class);
        List<MethodCallExpr> optionalQueryParams = methodCalls.stream()
                                                              .filter(call -> call.getNameAsString().equals("queryParam") ||
                                                                      call.getNameAsString().equals("queryParamAsClass"))
                                                              .collect(Collectors.toList());

        List<MethodCallExpr> requiredQueryParams = methodCalls.stream()
                                                              .filter(call -> call.getNameAsString().equals("requiredParam"))
                                                              .collect(Collectors.toList());

        List<MethodCallExpr> pathParams = methodCalls.stream()
                                                     .filter(call -> call.getNameAsString().equals("pathParam"))
                                                     .collect(Collectors.toList());

        List<OpenApiParamInfo> output = new ArrayList<>();

        return output;
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
