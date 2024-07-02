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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cwms.cda.data.dto.location.kind;

import static java.util.Comparator.comparing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = PhysicalStructureChange.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"project-id", "change-date", "protected", "discharge-computation-type", "reason-type", "notes"})
public final class PhysicalStructureChange<T extends Setting> extends CwmsDTO {
    @JsonProperty(required = true)
    private final CwmsId projectId;
    @JsonProperty(value = "protected", required = true)
    private final boolean isProtected;
    @JsonProperty(required = true)
    private final Instant changeDate;
    @JsonProperty(required = true)
    private final LookupType dischargeComputationType;
    @JsonProperty(required = true)
    private final LookupType reasonType;
    private final Double newTotalDischargeOverride;
    private final Double oldTotalDischargeOverride;
    private final String dischargeUnits;
    private final Double poolElevation;
    private final Double tailwaterElevation;
    private final String elevationUnits;
    private final String notes;
    private final Set<T> settings;

    private PhysicalStructureChange(Builder<T> builder) {
        super(null);
        this.projectId = builder.projectId;
        this.dischargeComputationType = builder.dischargeComputationType;
        this.reasonType = builder.reasonType;
        this.isProtected = builder.isProtected;
        this.newTotalDischargeOverride = builder.newTotalDischargeOverride;
        this.oldTotalDischargeOverride = builder.oldTotalDischargeOverride;
        this.dischargeUnits = builder.dischargeUnits;
        this.poolElevation = builder.poolElevation;
        this.tailwaterElevation = builder.tailwaterElevation;
        this.elevationUnits = builder.elevationUnits;
        this.notes = builder.notes;
        this.changeDate = builder.changeDate;
        this.settings = builder.settings;
    }

    @JsonIgnore
    @Override
    public String getOfficeId() {
        return projectId.getOfficeId();
    }

    public CwmsId getProjectId() {
        return projectId;
    }

    public LookupType getDischargeComputationType() {
        return dischargeComputationType;
    }

    public LookupType getReasonType() {
        return reasonType;
    }

    @JsonProperty("protected")
    public boolean isProtected() {
        return isProtected;
    }

    public Double getNewTotalDischargeOverride() {
        return newTotalDischargeOverride;
    }

    public Double getOldTotalDischargeOverride() {
        return oldTotalDischargeOverride;
    }

    public String getDischargeUnits() {
        return dischargeUnits;
    }

    public Double getPoolElevation() {
        return poolElevation;
    }

    public Double getTailwaterElevation() {
        return tailwaterElevation;
    }

    public String getElevationUnits() {
        return elevationUnits;
    }

    public String getNotes() {
        return notes;
    }

    public Instant getChangeDate() {
        return changeDate;
    }

    public Set<T> getSettings() {
        return settings;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.required(getProjectId(), "project-id");
        validator.required(getChangeDate(), "change-date");
        validator.required(getDischargeComputationType(), "discharge-computation-type");
        validator.required(getReasonType(), "reason-type");
    }

    public static final class Builder<T extends Setting> {
        private CwmsId projectId;
        private LookupType dischargeComputationType;
        private LookupType reasonType;
        @JsonProperty("protected")
        private boolean isProtected;
        private Double newTotalDischargeOverride;
        private Double oldTotalDischargeOverride;
        private String dischargeUnits;
        private Double poolElevation;
        private Double tailwaterElevation;
        private String elevationUnits;
        private String notes;
        private Set<T> settings;
        private Instant changeDate;


        public Builder<T> withProjectId(CwmsId projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder<T> withDischargeComputationType(LookupType dischargeComputationType) {
            this.dischargeComputationType = dischargeComputationType;
            return this;
        }

        public Builder<T> withReasonType(LookupType reasonType) {
            this.reasonType = reasonType;
            return this;
        }

        @JsonProperty("protected")
        public Builder<T> withProtected(boolean isProtected) {
            this.isProtected = isProtected;
            return this;
        }

        public Builder<T> withNewTotalDischargeOverride(Double newTotalDischargeOverride) {
            this.newTotalDischargeOverride = newTotalDischargeOverride;
            return this;
        }

        public Builder<T> withOldTotalDischargeOverride(Double oldTotalDischargeOverride) {
            this.oldTotalDischargeOverride = oldTotalDischargeOverride;
            return this;
        }

        public Builder<T> withDischargeUnits(String dischargeUnits) {
            this.dischargeUnits = dischargeUnits;
            return this;
        }

        public Builder<T> withPoolElevation(Double poolElevation) {
            this.poolElevation = poolElevation;
            return this;
        }

        public Builder<T> withTailwaterElevation(Double tailwaterElevation) {
            this.tailwaterElevation = tailwaterElevation;
            return this;
        }

        public Builder<T> withElevationUnits(String elevationUnits) {
            this.elevationUnits = elevationUnits;
            return this;
        }

        public Builder<T> withNotes(String notes) {
            this.notes = notes;
            return this;
        }

        public Builder<T> withChangeDate(Instant changeDate) {
            this.changeDate = changeDate;
            return this;
        }

        public Builder<T> withSettings(Set<T> settings) {
            this.settings = new TreeSet<>(comparing(Setting::getLocationId,
                comparing(CwmsId::getOfficeId).thenComparing(CwmsId::getName)));
            this.settings.addAll(settings);
            return this;
        }

        public PhysicalStructureChange<T> build() {
            return new PhysicalStructureChange<>(this);
        }
    }
}
