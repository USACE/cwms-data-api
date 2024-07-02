package cwms.cda.data.dto.basin;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Basin.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Basin implements CwmsDTOBase {
    private final CwmsId basinId;
    private final Double sortOrder;
    private final Double totalDrainageArea;
    private final Double contributingDrainageArea;
    private final CwmsId parentBasinId;
    private final String areaUnit;
    private final CwmsId primaryStreamId;

    private Basin(Builder builder) {
        this.basinId = builder.basinId;
        this.sortOrder = builder.sortOrder;
        this.totalDrainageArea = builder.totalDrainageArea;
        this.contributingDrainageArea = builder.contributingDrainageArea;
        this.parentBasinId = builder.parentBasinId;
        this.areaUnit = builder.areaUnit;
        this.primaryStreamId = builder.primaryStreamId;
    }

    public CwmsId getBasinId() {
        return basinId;
    }

    public CwmsId getPrimaryStreamId() {
        return primaryStreamId;
    }

    public Double getSortOrder() {
        return sortOrder;
    }

    public Double getTotalDrainageArea() {
        return totalDrainageArea;
    }

    public Double getContributingDrainageArea() {
        return contributingDrainageArea;
    }

    public CwmsId getParentBasinId() {
        return parentBasinId;
    }

    public String getAreaUnit() {
        return areaUnit;
    }

    public static class Builder {
        private CwmsId basinId;
        private Double sortOrder;
        private Double totalDrainageArea;
        private Double contributingDrainageArea;
        private CwmsId parentBasinId;
        private String areaUnit;
        private CwmsId primaryStreamId;

        public Builder withBasinId(CwmsId basinId) {
            this.basinId = basinId;
            return this;
        }

        public Builder withPrimaryStreamId(CwmsId primaryStreamId) {
            this.primaryStreamId = primaryStreamId;
            return this;
        }

        public Builder withSortOrder(Double sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder withTotalDrainageArea(Double totalDrainageArea) {
            this.totalDrainageArea = totalDrainageArea;
            return this;
        }

        public Builder withContributingDrainageArea(Double contributingDrainageArea) {
            this.contributingDrainageArea = contributingDrainageArea;
            return this;
        }

        public Builder withParentBasinId(CwmsId parentBasinId) {
            this.parentBasinId = parentBasinId;
            return this;
        }

        public Builder withAreaUnit(String areaUnit) {
            this.areaUnit = areaUnit;
            return this;
        }

        public Basin build() {
            return new Basin(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        if (this.basinId == null) {
            throw new FieldException("Basin identifier field can't be null");
        }
        basinId.validate();
    }
}