/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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
package cwms.cda.data.dto.measurement;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Measurement.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Measurement extends CwmsDTOBase {
    private final String heightUnit;
    private final String flowUnit;
    private final String tempUnit;
    private final String velocityUnit;
    private final String areaUnit;
    private final boolean used;
    private final String agency;
    private final String party;
    private final String wmComments;
    @JsonProperty(required = true)
    private final Instant instant;
    @JsonProperty(required = true)
    private final CwmsId id;
    @JsonProperty(required = true)
    private final String number;
    private final StreamflowMeasurement streamflowMeasurement;
    private final SupplementalStreamflowMeasurement supplementalStreamflowMeasurement;
    private final UsgsMeasurement usgsMeasurement;

    private Measurement(Builder builder) {
        this.heightUnit = builder.heightUnit;
        this.flowUnit = builder.flowUnit;
        this.tempUnit = builder.tempUnit;
        this.velocityUnit = builder.velocityUnit;
        this.areaUnit = builder.areaUnit;
        this.used = builder.used;
        this.agency = builder.agency;
        this.party = builder.party;
        this.wmComments = builder.wmComments;
        this.streamflowMeasurement = builder.streamflowMeasurement;
        this.supplementalStreamflowMeasurement = builder.supplementalStreamflowMeasurement;
        this.usgsMeasurement = builder.usgsMeasurement;
        this.id = builder.id;
        this.number = builder.number;
        this.instant = builder.instant;
    }

    public String getHeightUnit() {
        return heightUnit;
    }

    public String getFlowUnit() {
        return flowUnit;
    }

    public String getTempUnit() {
        return tempUnit;
    }

    public String getVelocityUnit() {
        return velocityUnit;
    }

    public String getAreaUnit() {
        return areaUnit;
    }

    public boolean isUsed() {
        return used;
    }

    public String getAgency() {
        return agency;
    }

    public String getParty() {
        return party;
    }

    public String getWmComments() {
        return wmComments;
    }

    @JsonIgnore
    public String getOfficeId() {
        return id.getOfficeId();
    }

    @JsonIgnore
    public String getLocationId() {
        return id.getName();
    }

    public CwmsId getId() {
        return id;
    }

    public Instant getInstant() {
        return instant;
    }

    public String getNumber() {
        return number;
    }

    public StreamflowMeasurement getStreamflowMeasurement() {
        return streamflowMeasurement;
    }

    public SupplementalStreamflowMeasurement getSupplementalStreamflowMeasurement() {
        return supplementalStreamflowMeasurement;
    }

    public UsgsMeasurement getUsgsMeasurement() {
        return usgsMeasurement;
    }

    public static final class Builder {
        private String heightUnit;
        private String flowUnit;
        private String tempUnit;
        private String velocityUnit;
        private String areaUnit;
        private boolean used;
        private String agency;
        private String party;
        private String wmComments;
        private StreamflowMeasurement streamflowMeasurement;
        private SupplementalStreamflowMeasurement supplementalStreamflowMeasurement;
        private UsgsMeasurement usgsMeasurement;
        private Instant instant;
        private String number;
        private CwmsId id;

        public Builder withHeightUnit(String heightUnit) {
            this.heightUnit = heightUnit;
            return this;
        }

        public Builder withFlowUnit(String flowUnit) {
            this.flowUnit = flowUnit;
            return this;
        }

        public Builder withTempUnit(String tempUnit) {
            this.tempUnit = tempUnit;
            return this;
        }

        public Builder withVelocityUnit(String velocityUnit) {
            this.velocityUnit = velocityUnit;
            return this;
        }

        public Builder withAreaUnit(String areaUnit) {
            this.areaUnit = areaUnit;
            return this;
        }

        public Builder withUsed(boolean used) {
            this.used = used;
            return this;
        }

        public Builder withAgency(String agency) {
            this.agency = agency;
            return this;
        }

        public Builder withParty(String party) {
            this.party = party;
            return this;
        }

        public Builder withWmComments(String wmComments) {
            this.wmComments = wmComments;
            return this;
        }

        public Builder withStreamflowMeasurement(StreamflowMeasurement streamflowMeasurement) {
            this.streamflowMeasurement = streamflowMeasurement;
            return this;
        }

        public Builder withSupplementalStreamflowMeasurement(SupplementalStreamflowMeasurement supplementalStreamflowMeasurement) {
            this.supplementalStreamflowMeasurement = supplementalStreamflowMeasurement;
            return this;
        }

        public Builder withUsgsMeasurement(UsgsMeasurement usgsMeasurement) {
            this.usgsMeasurement = usgsMeasurement;
            return this;
        }

        public Builder withId(CwmsId id) {
            this.id = id;
            return this;
        }

        public Builder withNumber(String number) {
            this.number = number;
            return this;
        }

        public Builder withInstant(Instant instant) {
            this.instant = instant;
            return this;
        }

        public Measurement build() {
            return new Measurement(this);
        }
    }
}
