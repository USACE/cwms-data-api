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
    private final Double basinArea;
    private final Double contributingArea;
    private final LocationIdentifier parentBasinId;
    private final String areaUnit;
    private final LocationIdentifier primaryStreamId;

    private Basin(Builder builder)
    {
        this.basinId = builder.basinId;
        this.sortOrder = builder.sortOrder;
        this.basinArea = builder.basinArea;
        this.contributingArea = builder.contributingArea;
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
        return basinArea;
    }

    public Double getContributingArea()
    {
        return contributingArea;
    }

    public LocationIdentifier getParentBasinId()
    {
        return parentBasinId;
    }

    public String getAreaUnit()
    {
        return areaUnit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Basin basin = (Basin) o;
        return Objects.equals(getBasinId(), basin.basinId)
                && Objects.equals(getPrimaryStreamId(), basin.primaryStreamId)
                && Objects.equals(getSortOrder(), basin.sortOrder)
                && Objects.equals(getBasinArea(), basin.basinArea)
                && Objects.equals(getContributingArea(), basin.contributingArea)
                && Objects.equals(getParentBasinId(), basin.parentBasinId)
                && Objects.equals(getAreaUnit(), basin.areaUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBasinId(), getPrimaryStreamId(), getSortOrder(), getBasinArea(),
                getContributingArea(), getParentBasinId(), getAreaUnit());
    }

    public static class Builder
    {
        private LocationIdentifier basinId;
        private Double sortOrder;
        private Double basinArea;
        private Double contributingArea;
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

        public Builder withBasinArea(Double basinArea)
        {
            this.basinArea = basinArea;
            return this;
        }

        public Builder withContributingArea(Double contributingArea)
        {
            this.contributingArea = contributingArea;
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
    public void validate() throws FieldException {
        if (this.basinId == null) {
            throw new FieldException("Basin identifier field can't be null");
        }
    }
}
