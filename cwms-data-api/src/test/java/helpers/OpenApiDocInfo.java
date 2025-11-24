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

package helpers;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class OpenApiDocInfo {
    private final Method method;
    private final List<OpenApiParamInfo> queryParameters = new ArrayList<>();
    private final List<OpenApiParamInfo> pathParameters = new ArrayList<>();
    private final boolean ignored;

    public OpenApiDocInfo(Method method, boolean ignored) {
        this.method = method;
        this.ignored = ignored;
    }

    public Method getMethod() {
        return method;
    }

    public List<OpenApiParamInfo> getPathParameters() {
        return pathParameters;
    }

    public List<OpenApiParamInfo> getQueryParameters() {
        return queryParameters;
    }

    public boolean isIgnored() {
        return ignored;
    }

    @Override
    public String toString() {
        String temp = ignored ? " - Ignored" : "";
        return method.getName() + temp;
    }
}
