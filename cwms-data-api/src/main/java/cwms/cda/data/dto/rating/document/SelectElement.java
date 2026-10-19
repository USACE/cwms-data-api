/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.rating.document;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.List;

@JsonDeserialize(builder = SelectElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"case", "default"})
public final class SelectElement {

    @JsonProperty("case")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<CaseElement> cases;

    @JsonProperty("default")
    private final String defaultValue;

    private SelectElement(Builder builder) {
        this.cases = ListUtil.copyOf(builder.cases);
        this.defaultValue = builder.defaultValue;
    }

    public List<CaseElement> getCases() {
        return cases;
    }

    public String getDefault() {
        return defaultValue;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private List<CaseElement> cases;
        private String defaultValue;

        @JsonProperty("case")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withCases(List<CaseElement> cases) {
            this.cases = cases;
            return this;
        }

        @JsonProperty("default")
        public Builder withDefault(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public SelectElement build() {
            return new SelectElement(this);
        }
    }
}
