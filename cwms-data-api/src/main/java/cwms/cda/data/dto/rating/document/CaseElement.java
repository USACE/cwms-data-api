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

@JsonDeserialize(builder = CaseElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"position", "when", "then"})
public final class CaseElement {

    @JsonProperty("position")
    @JacksonXmlProperty(isAttribute = true, localName = "position")
    private final String position;

    @JsonProperty("when")
    private final String when;

    @JsonProperty("then")
    private final String then;

    private CaseElement(Builder builder) {
        this.position = builder.position;
        this.when = builder.when;
        this.then = builder.then;
    }

    public String getPosition() {
        return position;
    }

    public String getWhen() {
        return when;
    }

    public String getThen() {
        return then;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String position;
        private String when;
        private String then;

        @JsonProperty("position")
        public Builder withPosition(String position) {
            this.position = position;
            return this;
        }

        @JsonIgnore
        public Builder withPosition(int position) {
            return withPosition(String.valueOf(position));
        }

        @JsonProperty("when")
        public Builder withWhen(String when) {
            this.when = when;
            return this;
        }

        @JsonProperty("then")
        public Builder withThen(String then) {
            this.then = then;
            return this;
        }

        public CaseElement build() {
            return new CaseElement(this);
        }
    }
}
