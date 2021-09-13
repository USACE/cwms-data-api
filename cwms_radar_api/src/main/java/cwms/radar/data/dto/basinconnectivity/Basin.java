package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

public final class Basin implements CwmsDTO
{
    private final String basinName;
    private final String officeId;
    private final Stream primaryStream;
    private final Double sortOrder;
    private final Double basinArea;
    private final Double contributingArea;
    private final String parentBasinId;

    private Basin(Builder builder)
    {
        basinName = builder.basinName;
        primaryStream = builder.primaryStream;
        officeId = builder.officeId;
        sortOrder = builder.sortOrder;
        basinArea = builder.basinArea;
        contributingArea = builder.contributingArea;
        parentBasinId = builder.parentBasinId;
    }

    public String getBasinName()
    {
        return basinName;
    }

    public Stream getPrimaryStream()
    {
        return primaryStream;
    }

    public String getOfficeId()
    {
        return officeId;
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

    public String getParentBasinId()
    {
        return parentBasinId;
    }

    public static class Builder
    {
        private final String basinName;
        private final String officeId;
        private Stream primaryStream;
        private Double sortOrder;
        private Double basinArea;
        private Double contributingArea;
        private String parentBasinId;

        public Builder(Basin basin)
        {
            this.basinName = basin.getBasinName();
            this.officeId = basin.getOfficeId();
            this.primaryStream = basin.getPrimaryStream();
        }

        public Builder(String basinName, String officeId)
        {
            this.basinName = basinName;
            this.officeId = officeId;
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

        public Builder withParentBasinId(String parentBasinId)
        {
            this.parentBasinId = parentBasinId;
            return this;
        }

        public Builder withPrimaryStream(Stream primaryStream)
        {
            this.primaryStream = primaryStream;
            return this;
        }

        public Basin build()
        {
            return new Basin(this);
        }
    }
}
