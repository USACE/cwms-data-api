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

public class OpenApiParamUsageInfo {
    private final OpenApiParamInfo paramInfo;
    private final boolean used;
    private final boolean nullHandled;

    public OpenApiParamUsageInfo(OpenApiParamInfo paramInfo, boolean used, boolean nullHandled) {
        this.paramInfo = paramInfo;
        this.used = used;
        this.nullHandled = nullHandled;
    }

    public OpenApiParamInfo getParamInfo() {
        return paramInfo;
    }

    public boolean isUsed() {
        return used;
    }

    public boolean isNullHandled() {
        return nullHandled;
    }

    @Override
    public String toString() {
        String realUse = used ? "In use" : "Not Used";
        return paramInfo + " - " + realUse;
    }
}
