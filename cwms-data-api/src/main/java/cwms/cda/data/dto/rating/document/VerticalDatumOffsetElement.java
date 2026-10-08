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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

@JsonDeserialize(builder = VerticalDatumOffsetElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"estimate", "to-datum", "value"})
public final class VerticalDatumOffsetElement {

    @JsonProperty("estimate")
    @JacksonXmlProperty(isAttribute = true, localName = "estimate")
    private final String estimate;

    @JsonProperty("to-datum")
    private final String toDatum;

    @JsonProperty("value")
    private final String value;

    private VerticalDatumOffsetElement(Builder builder) {
        this.estimate = builder.estimate;
        this.toDatum = builder.toDatum;
        this.value = builder.value;
    }

    public String getEstimate() {
        return estimate;
    }

    public String getToDatum() {
        return toDatum;
    }

    public String getValue() {
        return value;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String estimate;
        private String toDatum;
        private String value;

        @JsonProperty("estimate")
        public Builder withEstimate(String estimate) {
            this.estimate = estimate;
            return this;
        }

        @JsonIgnore
        public Builder withEstimate(boolean estimate) {
            return withEstimate(String.valueOf(estimate));
        }

        @JsonProperty("to-datum")
        public Builder withToDatum(String toDatum) {
            this.toDatum = toDatum;
            return this;
        }

        @JsonProperty("value")
        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public VerticalDatumOffsetElement build() {
            return new VerticalDatumOffsetElement(this);
        }
    }
}
