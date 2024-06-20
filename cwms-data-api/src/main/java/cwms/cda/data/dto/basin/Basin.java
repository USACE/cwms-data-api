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
    private final LocationIdentifier BASIN_ID;
    private final Double SORT_ORDER;
    private final Double TOTAL_DRAINAGE_AREA;
    private final Double CONTRIBUTING_DRAINAGE_AREA;
    private final LocationIdentifier PARENT_BASIN_ID;
    private final String AREA_UNIT;
    private final LocationIdentifier PRIMARY_STREAM_ID;

    private Basin(Builder builder)
    {
        this.BASIN_ID = builder.BASIN_ID;
        this.SORT_ORDER = builder.SORT_ORDER;
        this.TOTAL_DRAINAGE_AREA = builder.TOTAL_DRAINAGE_AREA;
        this.CONTRIBUTING_DRAINAGE_AREA = builder.CONTRIBUTING_DRAINAGE_AREA;
        this.PARENT_BASIN_ID = builder.PARENT_BASIN_ID;
        this.AREA_UNIT = builder.AREA_UNIT;
        this.PRIMARY_STREAM_ID = builder.PRIMARY_STREAM_ID;
    }

    public LocationIdentifier getBasinId()
    {
        return BASIN_ID;
    }

    public LocationIdentifier getPrimaryStreamId()
    {
        return PRIMARY_STREAM_ID;
    }

    public Double getSortOrder()
    {
        return SORT_ORDER;
    }

    public Double getBasinArea()
    {
        return TOTAL_DRAINAGE_AREA;
    }

    public Double getContributingArea()
    {
        return CONTRIBUTING_DRAINAGE_AREA;
    }

    public LocationIdentifier getParentBasinId()
    {
        return PARENT_BASIN_ID;
    }

    public String getAreaUnit()
    {
        return AREA_UNIT;
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
        return Objects.equals(getBasinId(), basin.BASIN_ID)
                && Objects.equals(getPrimaryStreamId(), basin.PRIMARY_STREAM_ID)
                && Objects.equals(getSortOrder(), basin.SORT_ORDER)
                && Objects.equals(getBasinArea(), basin.TOTAL_DRAINAGE_AREA)
                && Objects.equals(getContributingArea(), basin.CONTRIBUTING_DRAINAGE_AREA)
                && Objects.equals(getParentBasinId(), basin.PARENT_BASIN_ID)
                && Objects.equals(getAreaUnit(), basin.AREA_UNIT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBasinId(), getPrimaryStreamId(), getSortOrder(), getBasinArea(),
                getContributingArea(), getParentBasinId(), getAreaUnit());
    }

    public static class Builder
    {
        private LocationIdentifier BASIN_ID;
        private Double SORT_ORDER;
        private Double TOTAL_DRAINAGE_AREA;
        private Double CONTRIBUTING_DRAINAGE_AREA;
        private LocationIdentifier PARENT_BASIN_ID;
        private String AREA_UNIT;
        private LocationIdentifier PRIMARY_STREAM_ID;

        public Builder withBasinId(LocationIdentifier BASIN_ID)
        {
            this.BASIN_ID = BASIN_ID;
            return this;
        }

        public Builder withPrimaryStreamId(LocationIdentifier PRIMARY_STREAM_ID)
        {
            this.PRIMARY_STREAM_ID = PRIMARY_STREAM_ID;
            return this;
        }

        public Builder withSortOrder(Double SORT_ORDER)
        {
            this.SORT_ORDER = SORT_ORDER;
            return this;
        }

        public Builder withBasinArea(Double TOTAL_DRAINAGE_AREA)
        {
            this.TOTAL_DRAINAGE_AREA = TOTAL_DRAINAGE_AREA;
            return this;
        }

        public Builder withContributingArea(Double CONTRIBUTING_DRAINAGE_AREA)
        {
            this.CONTRIBUTING_DRAINAGE_AREA = CONTRIBUTING_DRAINAGE_AREA;
            return this;
        }

        public Builder withParentBasinId(LocationIdentifier PARENT_BASIN_ID)
        {
            this.PARENT_BASIN_ID = PARENT_BASIN_ID;
            return this;
        }

        public Builder withAreaUnit(String AREA_UNIT)
        {
            this.AREA_UNIT = AREA_UNIT;
            return this;
        }

        public Basin build()
        {
            return new Basin(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        if (this.BASIN_ID == null) {
            throw new FieldException("Basin identifier field can't be null");
        }
        BASIN_ID.validate();
    }
}
