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

import java.util.Objects;
import java.util.Set;

public class OpenApiParamUsage {
    private final Set<OpenApiParamUsageInfo> queryParams;
    private final Set<OpenApiParamUsageInfo> pathParams;
    private final OpenApiParamUsageInfo resourceId;

    public OpenApiParamUsage(Set<OpenApiParamUsageInfo> pathParams, Set<OpenApiParamUsageInfo> queryParams,
                             OpenApiParamUsageInfo resourceId) {
        this.pathParams = pathParams;
        this.queryParams = queryParams;
        this.resourceId = resourceId;
    }

    public Set<OpenApiParamUsageInfo> getPathParams() {
        return pathParams;
    }

    public Set<OpenApiParamUsageInfo> getQueryParams() {
        return queryParams;
    }

    public OpenApiParamUsageInfo getResourceId() {
        return resourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OpenApiParamUsage)) {
            return false;
        }
        OpenApiParamUsage that = (OpenApiParamUsage) o;
        return Objects.equals(getQueryParams(), that.getQueryParams()) && Objects.equals(getPathParams(),
                                                                                         that.getPathParams()) && Objects.equals(
                getResourceId(), that.getResourceId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getQueryParams(), getPathParams(), getResourceId());
    }
}
