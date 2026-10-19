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

@JsonDeserialize(builder = RatingPointsElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"other-ind", "point"})
public final class RatingPointsElement {

    @JsonProperty("other-ind")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<OtherIndElement> otherInds;

    @JsonProperty("point")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<PointElement> points;

    private RatingPointsElement(Builder builder) {
        this.otherInds = ListUtil.copyOf(builder.otherInds);
        this.points = ListUtil.copyOf(builder.points);
    }

    public List<OtherIndElement> getOtherInds() {
        return otherInds;
    }

    public List<PointElement> getPoints() {
        return points;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private List<OtherIndElement> otherInds;
        private List<PointElement> points;

        @JsonProperty("other-ind")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withOtherInds(List<OtherIndElement> otherInds) {
            this.otherInds = otherInds;
            return this;
        }

        @JsonProperty("point")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withPoints(List<PointElement> points) {
            this.points = points;
            return this;
        }

        public RatingPointsElement build() {
            return new RatingPointsElement(this);
        }
    }
}
