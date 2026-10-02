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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;

@JsonDeserialize(builder = PositionalValueElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({PositionalValueElement.POSITION, PositionalValueElement.ELEMENT_VALUE})
public final class PositionalValueElement {

    static final String POSITION = "position";
    static final String ELEMENT_VALUE = "element-value";

    @JsonProperty(POSITION)
    @JacksonXmlProperty(isAttribute = true, localName = POSITION)
    private final String position;

    @JsonProperty(ELEMENT_VALUE)
    @JacksonXmlText
    private final String value;

    private PositionalValueElement(Builder builder) {
        this.position = builder.position;
        this.value = builder.value;
    }

    public String getPosition() {
        return position;
    }

    public String getValue() {
        return value;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String position;
        private String value;

        @JsonProperty(POSITION)
        public Builder withPosition(String position) {
            this.position = position;
            return this;
        }

        @JsonIgnore
        public Builder withPosition(int position) {
            return withPosition(String.valueOf(position));
        }

        @JsonProperty(ELEMENT_VALUE)
        @JacksonXmlText
        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        public PositionalValueElement build() {
            return new PositionalValueElement(this);
        }
    }
}
