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

@JsonDeserialize(builder = VirtualRatingElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"office-id", "rating-spec-id", "effective-date", "transition-start-date",
    "create-date", "active", "description", "connections", "source-ratings"})
public final class VirtualRatingElement {

    @JsonProperty("office-id")
    @JacksonXmlProperty(isAttribute = true, localName = "office-id")
    private final String officeId;

    @JsonProperty("rating-spec-id")
    private final String ratingSpecId;

    @JsonProperty("effective-date")
    private final String effectiveDate;

    @JsonProperty("transition-start-date")
    private final String transitionStartDate;

    @JsonProperty("create-date")
    private final String createDate;

    @JsonProperty("active")
    private final String active;

    @JsonProperty("description")
    private final String description;

    @JsonProperty("connections")
    private final String connections;

    @JsonProperty("source-ratings")
    private final VirtualSourceRatingsElement sourceRatings;

    private VirtualRatingElement(Builder builder) {
        this.officeId = builder.officeId;
        this.ratingSpecId = builder.ratingSpecId;
        this.effectiveDate = builder.effectiveDate;
        this.transitionStartDate = builder.transitionStartDate;
        this.createDate = builder.createDate;
        this.active = builder.active;
        this.description = builder.description;
        this.connections = builder.connections;
        this.sourceRatings = builder.sourceRatings;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getRatingSpecId() {
        return ratingSpecId;
    }

    public String getEffectiveDate() {
        return effectiveDate;
    }

    public String getTransitionStartDate() {
        return transitionStartDate;
    }

    public String getCreateDate() {
        return createDate;
    }

    public String getActive() {
        return active;
    }

    public String getDescription() {
        return description;
    }

    public String getConnections() {
        return connections;
    }

    public VirtualSourceRatingsElement getSourceRatings() {
        return sourceRatings;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String officeId;
        private String ratingSpecId;
        private String effectiveDate;
        private String transitionStartDate;
        private String createDate;
        private String active;
        private String description;
        private String connections;
        private VirtualSourceRatingsElement sourceRatings;

        @JsonProperty("office-id")
        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        @JsonProperty("rating-spec-id")
        public Builder withRatingSpecId(String ratingSpecId) {
            this.ratingSpecId = ratingSpecId;
            return this;
        }

        @JsonProperty("effective-date")
        public Builder withEffectiveDate(String effectiveDate) {
            this.effectiveDate = effectiveDate;
            return this;
        }

        @JsonProperty("transition-start-date")
        public Builder withTransitionStartDate(String transitionStartDate) {
            this.transitionStartDate = transitionStartDate;
            return this;
        }

        @JsonProperty("create-date")
        public Builder withCreateDate(String createDate) {
            this.createDate = createDate;
            return this;
        }

        @JsonProperty("active")
        public Builder withActive(String active) {
            this.active = active;
            return this;
        }

        @JsonIgnore
        public Builder withActive(boolean active) {
            return withActive(String.valueOf(active));
        }

        @JsonProperty("description")
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        @JsonProperty("connections")
        public Builder withConnections(String connections) {
            this.connections = connections;
            return this;
        }

        @JsonProperty("source-ratings")
        public Builder withSourceRatings(VirtualSourceRatingsElement sourceRatings) {
            this.sourceRatings = sourceRatings;
            return this;
        }

        public VirtualRatingElement build() {
            return new VirtualRatingElement(this);
        }
    }
}
