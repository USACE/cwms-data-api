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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

@JsonDeserialize(builder = RatingTemplateElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"office-id", "parameters-id", "version", "ind-parameter-specs", "dep-parameter",
    "description"})
public final class RatingTemplateElement {

    @JsonProperty("office-id")
    @JacksonXmlProperty(isAttribute = true, localName = "office-id")
    private final String officeId;

    @JsonProperty("parameters-id")
    private final String parametersId;

    @JsonProperty("version")
    private final String version;

    @JsonProperty("ind-parameter-specs")
    private final IndParameterSpecsElement indParameterSpecs;

    @JsonProperty("dep-parameter")
    private final String depParameter;

    @JsonProperty("description")
    private final String description;

    private RatingTemplateElement(Builder builder) {
        this.officeId = builder.officeId;
        this.parametersId = builder.parametersId;
        this.version = builder.version;
        this.indParameterSpecs = builder.indParameterSpecs;
        this.depParameter = builder.depParameter;
        this.description = builder.description;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getParametersId() {
        return parametersId;
    }

    public String getVersion() {
        return version;
    }

    public IndParameterSpecsElement getIndParameterSpecs() {
        return indParameterSpecs;
    }

    public String getDepParameter() {
        return depParameter;
    }

    public String getDescription() {
        return description;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String officeId;
        private String parametersId;
        private String version;
        private IndParameterSpecsElement indParameterSpecs;
        private String depParameter;
        private String description;

        @JsonProperty("office-id")
        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        @JsonProperty("parameters-id")
        public Builder withParametersId(String parametersId) {
            this.parametersId = parametersId;
            return this;
        }

        @JsonProperty("version")
        public Builder withVersion(String version) {
            this.version = version;
            return this;
        }

        @JsonProperty("ind-parameter-specs")
        public Builder withIndParameterSpecs(IndParameterSpecsElement indParameterSpecs) {
            this.indParameterSpecs = indParameterSpecs;
            return this;
        }

        @JsonProperty("dep-parameter")
        public Builder withDepParameter(String depParameter) {
            this.depParameter = depParameter;
            return this;
        }

        @JsonProperty("description")
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public RatingTemplateElement build() {
            return new RatingTemplateElement(this);
        }
    }
}
