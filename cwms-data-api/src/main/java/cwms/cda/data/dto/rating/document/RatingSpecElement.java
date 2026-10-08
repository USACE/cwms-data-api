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

@JsonDeserialize(builder = RatingSpecElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"office-id", "rating-spec-id", "template-id", "location-id", "version",
    "source-agency", "in-range-method", "out-range-low-method", "out-range-high-method", "active",
    "auto-update", "auto-activate", "auto-migrate-extension", "ind-rounding-specs",
    "dep-rounding-spec", "description"})
public final class RatingSpecElement {

    @JsonProperty("office-id")
    @JacksonXmlProperty(isAttribute = true, localName = "office-id")
    private final String officeId;

    @JsonProperty("rating-spec-id")
    private final String ratingSpecId;

    @JsonProperty("template-id")
    private final String templateId;

    @JsonProperty("location-id")
    private final String locationId;

    @JsonProperty("version")
    private final String version;

    @JsonProperty("source-agency")
    private final String sourceAgency;

    @JsonProperty("in-range-method")
    private final String inRangeMethod;

    @JsonProperty("out-range-low-method")
    private final String outRangeLowMethod;

    @JsonProperty("out-range-high-method")
    private final String outRangeHighMethod;

    @JsonProperty("active")
    private final String active;

    @JsonProperty("auto-update")
    private final String autoUpdate;

    @JsonProperty("auto-activate")
    private final String autoActivate;

    @JsonProperty("auto-migrate-extension")
    private final String autoMigrateExtension;

    @JsonProperty("ind-rounding-specs")
    private final IndRoundingSpecsElement indRoundingSpecs;

    @JsonProperty("dep-rounding-spec")
    private final String depRoundingSpec;

    @JsonProperty("description")
    private final String description;

    private RatingSpecElement(Builder builder) {
        this.officeId = builder.officeId;
        this.ratingSpecId = builder.ratingSpecId;
        this.templateId = builder.templateId;
        this.locationId = builder.locationId;
        this.version = builder.version;
        this.sourceAgency = builder.sourceAgency;
        this.inRangeMethod = builder.inRangeMethod;
        this.outRangeLowMethod = builder.outRangeLowMethod;
        this.outRangeHighMethod = builder.outRangeHighMethod;
        this.active = builder.active;
        this.autoUpdate = builder.autoUpdate;
        this.autoActivate = builder.autoActivate;
        this.autoMigrateExtension = builder.autoMigrateExtension;
        this.indRoundingSpecs = builder.indRoundingSpecs;
        this.depRoundingSpec = builder.depRoundingSpec;
        this.description = builder.description;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getRatingSpecId() {
        return ratingSpecId;
    }

    public String getTemplateId() {
        return templateId;
    }

    public String getLocationId() {
        return locationId;
    }

    public String getVersion() {
        return version;
    }

    public String getSourceAgency() {
        return sourceAgency;
    }

    public String getInRangeMethod() {
        return inRangeMethod;
    }

    public String getOutRangeLowMethod() {
        return outRangeLowMethod;
    }

    public String getOutRangeHighMethod() {
        return outRangeHighMethod;
    }

    public String getActive() {
        return active;
    }

    public String getAutoUpdate() {
        return autoUpdate;
    }

    public String getAutoActivate() {
        return autoActivate;
    }

    public String getAutoMigrateExtension() {
        return autoMigrateExtension;
    }

    public IndRoundingSpecsElement getIndRoundingSpecs() {
        return indRoundingSpecs;
    }

    public String getDepRoundingSpec() {
        return depRoundingSpec;
    }

    public String getDescription() {
        return description;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String officeId;
        private String ratingSpecId;
        private String templateId;
        private String locationId;
        private String version;
        private String sourceAgency;
        private String inRangeMethod;
        private String outRangeLowMethod;
        private String outRangeHighMethod;
        private String active;
        private String autoUpdate;
        private String autoActivate;
        private String autoMigrateExtension;
        private IndRoundingSpecsElement indRoundingSpecs;
        private String depRoundingSpec;
        private String description;

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

        @JsonProperty("template-id")
        public Builder withTemplateId(String templateId) {
            this.templateId = templateId;
            return this;
        }

        @JsonProperty("location-id")
        public Builder withLocationId(String locationId) {
            this.locationId = locationId;
            return this;
        }

        @JsonProperty("version")
        public Builder withVersion(String version) {
            this.version = version;
            return this;
        }

        @JsonProperty("source-agency")
        public Builder withSourceAgency(String sourceAgency) {
            this.sourceAgency = sourceAgency;
            return this;
        }

        @JsonProperty("in-range-method")
        public Builder withInRangeMethod(String inRangeMethod) {
            this.inRangeMethod = inRangeMethod;
            return this;
        }

        @JsonProperty("out-range-low-method")
        public Builder withOutRangeLowMethod(String outRangeLowMethod) {
            this.outRangeLowMethod = outRangeLowMethod;
            return this;
        }

        @JsonProperty("out-range-high-method")
        public Builder withOutRangeHighMethod(String outRangeHighMethod) {
            this.outRangeHighMethod = outRangeHighMethod;
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

        @JsonProperty("auto-update")
        public Builder withAutoUpdate(String autoUpdate) {
            this.autoUpdate = autoUpdate;
            return this;
        }

        @JsonIgnore
        public Builder withAutoUpdate(boolean autoUpdate) {
            return withAutoUpdate(String.valueOf(autoUpdate));
        }

        @JsonProperty("auto-activate")
        public Builder withAutoActivate(String autoActivate) {
            this.autoActivate = autoActivate;
            return this;
        }

        @JsonIgnore
        public Builder withAutoActivate(boolean autoActivate) {
            return withAutoActivate(String.valueOf(autoActivate));
        }

        @JsonProperty("auto-migrate-extension")
        public Builder withAutoMigrateExtension(String autoMigrateExtension) {
            this.autoMigrateExtension = autoMigrateExtension;
            return this;
        }

        @JsonIgnore
        public Builder withAutoMigrateExtension(boolean autoMigrateExtension) {
            return withAutoMigrateExtension(String.valueOf(autoMigrateExtension));
        }

        @JsonProperty("ind-rounding-specs")
        public Builder withIndRoundingSpecs(IndRoundingSpecsElement indRoundingSpecs) {
            this.indRoundingSpecs = indRoundingSpecs;
            return this;
        }

        @JsonProperty("dep-rounding-spec")
        public Builder withDepRoundingSpec(String depRoundingSpec) {
            this.depRoundingSpec = depRoundingSpec;
            return this;
        }

        @JsonProperty("description")
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public RatingSpecElement build() {
            return new RatingSpecElement(this);
        }
    }
}
