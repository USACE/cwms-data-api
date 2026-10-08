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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

@JsonDeserialize(builder = UsgsStreamRatingElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"office-id", "rating-spec-id", "vertical-datum-info", "units-id",
    "effective-date", "transition-start-date", "create-date", "active", "description",
    "height-shifts", "height-offsets", "rating-points", "extension-points"})
public final class UsgsStreamRatingElement {

    @JsonProperty("office-id")
    @JacksonXmlProperty(isAttribute = true, localName = "office-id")
    private final String officeId;

    @JsonProperty("rating-spec-id")
    private final String ratingSpecId;

    @JsonProperty("vertical-datum-info")
    private final VerticalDatumInfoElement verticalDatumInfo;

    @JsonProperty("units-id")
    private final String unitsId;

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

    @JsonProperty("height-shifts")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<HeightShiftsElement> heightShifts;

    @JsonProperty("height-offsets")
    private final HeightOffsetsElement heightOffsets;

    @JsonProperty("rating-points")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<RatingPointsElement> ratingPoints;

    @JsonProperty("extension-points")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<RatingPointsElement> extensionPoints;

    private UsgsStreamRatingElement(Builder builder) {
        this.officeId = builder.officeId;
        this.ratingSpecId = builder.ratingSpecId;
        this.verticalDatumInfo = builder.verticalDatumInfo;
        this.unitsId = builder.unitsId;
        this.effectiveDate = builder.effectiveDate;
        this.transitionStartDate = builder.transitionStartDate;
        this.createDate = builder.createDate;
        this.active = builder.active;
        this.description = builder.description;
        this.heightShifts = ListUtil.copyOf(builder.heightShifts);
        this.heightOffsets = builder.heightOffsets;
        this.ratingPoints = ListUtil.copyOf(builder.ratingPoints);
        this.extensionPoints = ListUtil.copyOf(builder.extensionPoints);
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getRatingSpecId() {
        return ratingSpecId;
    }

    public VerticalDatumInfoElement getVerticalDatumInfo() {
        return verticalDatumInfo;
    }

    public String getUnitsId() {
        return unitsId;
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

    public List<HeightShiftsElement> getHeightShifts() {
        return heightShifts;
    }

    public HeightOffsetsElement getHeightOffsets() {
        return heightOffsets;
    }

    public List<RatingPointsElement> getRatingPoints() {
        return ratingPoints;
    }

    public List<RatingPointsElement> getExtensionPoints() {
        return extensionPoints;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String officeId;
        private String ratingSpecId;
        private VerticalDatumInfoElement verticalDatumInfo;
        private String unitsId;
        private String effectiveDate;
        private String transitionStartDate;
        private String createDate;
        private String active;
        private String description;
        private List<HeightShiftsElement> heightShifts;
        private HeightOffsetsElement heightOffsets;
        private List<RatingPointsElement> ratingPoints;
        private List<RatingPointsElement> extensionPoints;

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

        @JsonProperty("vertical-datum-info")
        public Builder withVerticalDatumInfo(VerticalDatumInfoElement verticalDatumInfo) {
            this.verticalDatumInfo = verticalDatumInfo;
            return this;
        }

        @JsonProperty("units-id")
        public Builder withUnitsId(String unitsId) {
            this.unitsId = unitsId;
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

        @JsonProperty("height-shifts")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withHeightShifts(List<HeightShiftsElement> heightShifts) {
            this.heightShifts = heightShifts;
            return this;
        }

        @JsonProperty("height-offsets")
        public Builder withHeightOffsets(HeightOffsetsElement heightOffsets) {
            this.heightOffsets = heightOffsets;
            return this;
        }

        @JsonProperty("rating-points")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withRatingPoints(List<RatingPointsElement> ratingPoints) {
            this.ratingPoints = ratingPoints;
            return this;
        }

        @JsonProperty("extension-points")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withExtensionPoints(List<RatingPointsElement> extensionPoints) {
            this.extensionPoints = extensionPoints;
            return this;
        }

        public UsgsStreamRatingElement build() {
            return new UsgsStreamRatingElement(this);
        }
    }
}
