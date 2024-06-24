package cwms.cda.data.dto.basin;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Basin.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Basin implements CwmsDTOBase {
    private final LocationIdentifier basinId;
    private final Double sortOrder;
    private final Double totalDrainageArea;
    private final Double contributingDrainageArea;
    private final LocationIdentifier parentBasinId;
    private final String areaUnit;
    private final LocationIdentifier primaryStreamId;

    private Basin(Builder builder)
    {
        this.basinId = builder.basinId;
        this.sortOrder = builder.sortOrder;
        this.totalDrainageArea = builder.totalDrainageArea;
        this.contributingDrainageArea = builder.contributingDrainageArea;
        this.parentBasinId = builder.parentBasinId;
        this.areaUnit = builder.areaUnit;
        this.primaryStreamId = builder.primaryStreamId;
    }

    public LocationIdentifier getBasinId()
    {
        return basinId;
    }

    public LocationIdentifier getPrimaryStreamId()
    {
        return primaryStreamId;
    }

    public Double getSortOrder()
    {
        return sortOrder;
    }

    public Double getBasinArea()
    {
        return totalDrainageArea;
    }

    public Double getContributingArea()
    {
        return contributingDrainageArea;
    }

    public LocationIdentifier getParentBasinId()
    {
        return parentBasinId;
    }

    public String getAreaUnit()
    {
        return areaUnit;
    }

    public static class Builder
    {
        private LocationIdentifier basinId;
        private Double sortOrder;
        private Double totalDrainageArea;
        private Double contributingDrainageArea;
        private LocationIdentifier parentBasinId;
        private String areaUnit;
        private LocationIdentifier primaryStreamId;

        public Builder withBasinId(LocationIdentifier basinId)
        {
            this.basinId = basinId;
            return this;
        }

        public Builder withPrimaryStreamId(LocationIdentifier primaryStreamId)
        {
            this.primaryStreamId = primaryStreamId;
            return this;
        }

        public Builder withSortOrder(Double sortOrder)
        {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder withBasinArea(Double totalDrainageArea)
        {
            this.totalDrainageArea = totalDrainageArea;
            return this;
        }

        public Builder withContributingArea(Double contributingDrainageArea)
        {
            this.contributingDrainageArea = contributingDrainageArea;
            return this;
        }

        public Builder withParentBasinId(LocationIdentifier parentBasinId)
        {
            this.parentBasinId = parentBasinId;
            return this;
        }

        public Builder withAreaUnit(String areaUnit)
        {
            this.areaUnit = areaUnit;
            return this;
        }

        public Basin build()
        {
            return new Basin(this);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBasinId(), getSortOrder(), totalDrainageArea, contributingDrainageArea, getParentBasinId(), getAreaUnit(), getPrimaryStreamId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Basin basin = (Basin) o;
        return Objects.equals(getBasinId(), basin.getBasinId()) && Objects.equals(getSortOrder(), basin.getSortOrder()) && Objects.equals(totalDrainageArea, basin.totalDrainageArea) && Objects.equals(contributingDrainageArea, basin.contributingDrainageArea) && Objects.equals(getParentBasinId(), basin.getParentBasinId()) && Objects.equals(getAreaUnit(), basin.getAreaUnit()) && Objects.equals(getPrimaryStreamId(), basin.getPrimaryStreamId());
    }

    @Override
    public void validate() throws FieldException {
        if (this.basinId == null) {
            throw new FieldException("Basin identifier field can't be null");
        }
        basinId.validate();
    }
}
