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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.util.List;

@JacksonXmlRootElement(localName = RatingsDocument.ROOT_ELEMENT)
@JsonDeserialize(builder = RatingsDocument.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({RatingsDocument.SCHEMA_LOCATION, "rating-template", "rating-spec", "simple-rating",
    "usgs-stream-rating", "virtual-rating", "transitional-rating"})
public final class RatingsDocument {

    public static final String ROOT_ELEMENT = "ratings";
    static final String SCHEMA_LOCATION = "noNamespaceSchemaLocation";

    @JsonProperty(SCHEMA_LOCATION)
    @JacksonXmlProperty(isAttribute = true, localName = SCHEMA_LOCATION)
    private final String noNamespaceSchemaLocation;

    @JsonProperty("rating-template")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<RatingTemplateElement> ratingTemplates;

    @JsonProperty("rating-spec")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<RatingSpecElement> ratingSpecs;

    @JsonProperty("simple-rating")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<SimpleRatingElement> simpleRatings;

    @JsonProperty("usgs-stream-rating")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<UsgsStreamRatingElement> usgsStreamRatings;

    @JsonProperty("virtual-rating")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<VirtualRatingElement> virtualRatings;

    @JsonProperty("transitional-rating")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<TransitionalRatingElement> transitionalRatings;

    private RatingsDocument(Builder builder) {
        this.noNamespaceSchemaLocation = builder.noNamespaceSchemaLocation;
        this.ratingTemplates = ListUtil.copyOf(builder.ratingTemplates);
        this.ratingSpecs = ListUtil.copyOf(builder.ratingSpecs);
        this.simpleRatings = ListUtil.copyOf(builder.simpleRatings);
        this.usgsStreamRatings = ListUtil.copyOf(builder.usgsStreamRatings);
        this.virtualRatings = ListUtil.copyOf(builder.virtualRatings);
        this.transitionalRatings = ListUtil.copyOf(builder.transitionalRatings);
    }

    public String getNoNamespaceSchemaLocation() {
        return noNamespaceSchemaLocation;
    }

    public List<RatingTemplateElement> getRatingTemplates() {
        return ratingTemplates;
    }

    public List<RatingSpecElement> getRatingSpecs() {
        return ratingSpecs;
    }

    public List<SimpleRatingElement> getSimpleRatings() {
        return simpleRatings;
    }

    public List<UsgsStreamRatingElement> getUsgsStreamRatings() {
        return usgsStreamRatings;
    }

    public List<VirtualRatingElement> getVirtualRatings() {
        return virtualRatings;
    }

    public List<TransitionalRatingElement> getTransitionalRatings() {
        return transitionalRatings;
    }

    // Only the explicitly annotated add* methods are used by jackson.
    @JsonPOJOBuilder
    @JsonAutoDetect(setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonIgnoreProperties({"schemaLocation"})
    public static final class Builder {
        private String noNamespaceSchemaLocation;
        private List<RatingTemplateElement> ratingTemplates;
        private List<RatingSpecElement> ratingSpecs;
        private List<SimpleRatingElement> simpleRatings;
        private List<UsgsStreamRatingElement> usgsStreamRatings;
        private List<VirtualRatingElement> virtualRatings;
        private List<TransitionalRatingElement> transitionalRatings;

        public Builder() {
        }

        public Builder(RatingsDocument ratings) {
            this.noNamespaceSchemaLocation = ratings.noNamespaceSchemaLocation;
            this.ratingTemplates = ratings.ratingTemplates;
            this.ratingSpecs = ratings.ratingSpecs;
            this.simpleRatings = ratings.simpleRatings;
            this.usgsStreamRatings = ratings.usgsStreamRatings;
            this.virtualRatings = ratings.virtualRatings;
            this.transitionalRatings = ratings.transitionalRatings;
        }

        @JsonProperty(SCHEMA_LOCATION)
        public Builder withNoNamespaceSchemaLocation(String noNamespaceSchemaLocation) {
            this.noNamespaceSchemaLocation = noNamespaceSchemaLocation;
            return this;
        }

        public Builder withRatingTemplates(List<RatingTemplateElement> ratingTemplates) {
            this.ratingTemplates = ratingTemplates;
            return this;
        }

        public Builder withRatingSpecs(List<RatingSpecElement> ratingSpecs) {
            this.ratingSpecs = ratingSpecs;
            return this;
        }

        public Builder withSimpleRatings(List<SimpleRatingElement> simpleRatings) {
            this.simpleRatings = simpleRatings;
            return this;
        }

        public Builder withUsgsStreamRatings(List<UsgsStreamRatingElement> usgsStreamRatings) {
            this.usgsStreamRatings = usgsStreamRatings;
            return this;
        }

        public Builder withVirtualRatings(List<VirtualRatingElement> virtualRatings) {
            this.virtualRatings = virtualRatings;
            return this;
        }

        public Builder withTransitionalRatings(List<TransitionalRatingElement> transitionalRatings) {
            this.transitionalRatings = transitionalRatings;
            return this;
        }

        // Jackson calls these once per contiguous run of an element, so they append rather than
        // replace to support interleaved rating types.

        @JsonProperty("rating-template")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addRatingTemplates(List<RatingTemplateElement> values) {
            this.ratingTemplates = ListUtil.append(this.ratingTemplates, values);
            return this;
        }

        @JsonProperty("rating-spec")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addRatingSpecs(List<RatingSpecElement> values) {
            this.ratingSpecs = ListUtil.append(this.ratingSpecs, values);
            return this;
        }

        @JsonProperty("simple-rating")
        @JsonAlias("rating")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addSimpleRatings(List<SimpleRatingElement> values) {
            this.simpleRatings = ListUtil.append(this.simpleRatings, values);
            return this;
        }

        @JsonProperty("usgs-stream-rating")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addUsgsStreamRatings(List<UsgsStreamRatingElement> values) {
            this.usgsStreamRatings = ListUtil.append(this.usgsStreamRatings, values);
            return this;
        }

        @JsonProperty("virtual-rating")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addVirtualRatings(List<VirtualRatingElement> values) {
            this.virtualRatings = ListUtil.append(this.virtualRatings, values);
            return this;
        }

        @JsonProperty("transitional-rating")
        @JacksonXmlElementWrapper(useWrapping = false)
        private Builder addTransitionalRatings(List<TransitionalRatingElement> values) {
            this.transitionalRatings = ListUtil.append(this.transitionalRatings, values);
            return this;
        }

        public RatingsDocument build() {
            return new RatingsDocument(this);
        }
    }
}
