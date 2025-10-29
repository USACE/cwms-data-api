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
import java.util.HashSet;
import java.util.Set;

public class OpenApiDocInfo<T> {
    private final Method method;
    private final Class<T> clazz;
    private final Set<String> queryParameters = new HashSet<>();
    private final Set<String> pathParameters = new HashSet<>();

    public OpenApiDocInfo(Class<T> clazz, Method method) {
        this.clazz = clazz;
        this.method = method;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public Method getMethod() {
        return method;
    }

    public Set<String> getPathParameters() {
        return pathParameters;
    }

    public Set<String> getQueryParameters() {
        return queryParameters;
    }
}
