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

public class OpenApiParamInfo {
    private String name;
    private final boolean required;
    private final Class<?> type;

    public OpenApiParamInfo(String name, boolean required, Class<?> type) {
        this.name = name;
        this.required = required;
        this.type = type;
    }

    public OpenApiParamInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public boolean isRequired() {
        return required;
    }

    public Class<?> getType() {
        return type;
    }

    @Override
    public String toString() {
        String req = required ? " (required)" : "";
        String clazz = type != null ? type.getSimpleName() : "null";
        return clazz + " " + name + req;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OpenApiParamInfo)) {
            return false;
        }
        OpenApiParamInfo that = (OpenApiParamInfo) o;
        return Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName());
    }
}
