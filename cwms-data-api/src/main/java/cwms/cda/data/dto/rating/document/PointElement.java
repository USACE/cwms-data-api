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

@JsonDeserialize(builder = PointElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"ind", "dep", "note"})
public final class PointElement {

    @JsonProperty("ind")
    private final String ind;

    @JsonProperty("dep")
    private final String dep;

    @JsonProperty("note")
    private final String note;

    private PointElement(Builder builder) {
        this.ind = builder.ind;
        this.dep = builder.dep;
        this.note = builder.note;
    }

    public String getInd() {
        return ind;
    }

    public String getDep() {
        return dep;
    }

    public String getNote() {
        return note;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String ind;
        private String dep;
        private String note;

        @JsonProperty("ind")
        public Builder withInd(String ind) {
            this.ind = ind;
            return this;
        }

        @JsonIgnore
        public Builder withInd(double ind) {
            return withInd(String.valueOf(ind));
        }

        @JsonProperty("dep")
        public Builder withDep(String dep) {
            this.dep = dep;
            return this;
        }

        @JsonIgnore
        public Builder withDep(double dep) {
            return withDep(String.valueOf(dep));
        }

        @JsonProperty("note")
        public Builder withNote(String note) {
            this.note = note;
            return this;
        }

        public PointElement build() {
            return new PointElement(this);
        }
    }
}
