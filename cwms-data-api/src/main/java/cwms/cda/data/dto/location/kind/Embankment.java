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

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Embankment.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"projectId", "location", "structureType"})
public final class Embankment extends ProjectStructure {
    private final LookupType structureType;
    private final Double upstreamSideSlope;
    private final Double downstreamSideSlope;
    private final Double structureLength;
    private final Double heightMax;
    private final Double topWidth;
    private final String unitsId;
    private final LookupType downstreamProtectionType;
    private final LookupType upstreamProtectionType;

    private Embankment(Builder builder) {
        super(builder.projectId, builder.location);
        this.structureType = builder.structureType;
        this.upstreamProtectionType = builder.upstreamProtectionType;
        this.downstreamProtectionType = builder.downstreamProtectionType;
        this.unitsId = builder.unitsId;
        this.topWidth = builder.topWidth;
        this.heightMax = builder.heightMax;
        this.structureLength = builder.structureLength;
        this.downstreamSideSlope = builder.downstreamSideSlope;
        this.upstreamSideSlope = builder.upstreamSideSlope;
    }

    public Double getUpstreamSideSlope() {
        return upstreamSideSlope;
    }

    public Double getDownstreamSideSlope() {
        return downstreamSideSlope;
    }

    public Double getStructureLength() {
        return structureLength;
    }

    public Double getHeightMax() {
        return heightMax;
    }

    public Double getTopWidth() {
        return topWidth;
    }

    public String getUnitsId() {
        return unitsId;
    }

    public LookupType getStructureType() {
        return structureType;
    }

    public LookupType getUpstreamProtectionType() {
        return upstreamProtectionType;
    }

    public LookupType getDownstreamProtectionType() {
        return downstreamProtectionType;
    }

    @Override
    public void validate() throws FieldException {
        if (getLocation() == null) {
            throw new FieldException("Location field can't be null");
        }
        getLocation().validate();
        if (getProjectId() == null) {
            throw new FieldException("Project location Id field can't be null");
        }
        getProjectId().validate();
        if (this.structureType == null) {
            throw new FieldException("Structure type field can't be null");
        }
    }

    public static final class Builder {
        private Double upstreamSideSlope;
        private Double downstreamSideSlope;
        private Double structureLength;
        private Double heightMax;
        private Double topWidth;
        private String unitsId;
        private LookupType downstreamProtectionType;
        private LookupType upstreamProtectionType;
        private LookupType structureType;
        private Location location;
        private LocationIdentifier projectId;

        public Builder withUpstreamSideSlope(Double upstreamSideSlope) {
            this.upstreamSideSlope = upstreamSideSlope;
            return this;
        }

        public Builder withDownstreamSideSlope(Double downstreamSideSlope) {
            this.downstreamSideSlope = downstreamSideSlope;
            return this;
        }

        public Builder withStructureLength(Double structureLength) {
            this.structureLength = structureLength;
            return this;
        }

        public Builder withHeightMax(Double heightMax) {
            this.heightMax = heightMax;
            return this;
        }

        public Builder withTopWidth(Double topWidth) {
            this.topWidth = topWidth;
            return this;
        }

        public Builder withUnitsId(String unitsId) {
            this.unitsId = unitsId;
            return this;
        }

        public Builder withDownstreamProtectionType(LookupType downstreamProtectionType) {
            this.downstreamProtectionType = downstreamProtectionType;
            return this;
        }

        public Builder withUpstreamProtectionType(LookupType upstreamProtectionType) {
            this.upstreamProtectionType = upstreamProtectionType;
            return this;
        }

        public Builder withStructureType(LookupType structureType) {
            this.structureType = structureType;
            return this;
        }

        public Builder withLocation(Location location) {
            this.location = location;
            return this;
        }

        public Builder withProjectId(LocationIdentifier projectId) {
            this.projectId = projectId;
            return this;
        }

        public Embankment build() {
            return new Embankment(this);
        }
    }
}
