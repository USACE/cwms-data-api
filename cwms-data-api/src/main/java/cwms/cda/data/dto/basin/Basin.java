package cwms.cda.data.dto.basin;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.basinconnectivity.Stream;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public final class Basin implements CwmsDTOBase {
    private final String basinName;
    private final String officeId;
    private final Stream primaryStream;
    private final Double sortOrder;
    private final Double basinArea;
    private final Double contributingArea;
    private final String parentBasinId;
    private final LinkedList<Stream> streams;
    private final LinkedList<Stream> subBasins;

    private Basin(Builder builder)
    {
        this.officeId = builder.officeId;
        this.basinName = builder.basinName;
        this.primaryStream = builder.primaryStream;
        this.sortOrder = builder.sortOrder;
        this.basinArea = builder.basinArea;
        this.contributingArea = builder.contributingArea;
        this.parentBasinId = builder.parentBasinId;
        this.streams = builder.streams;
        this.subBasins = builder.subBasins;
    }

    public String getBasinName()
    {
        return basinName;
    }

    public String getOfficeId()
    {
        return officeId;
    }

    public Stream getPrimaryStream()
    {
        return primaryStream;
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

    public List<Stream> getStreams()
    {
        return streams;
    }

    public List<Stream> getSubBasins() {
        return subBasins;
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
        return Objects.equals(basinName, basin.basinName)
                && Objects.equals(primaryStream, basin.primaryStream)
                && Objects.equals(sortOrder, basin.sortOrder)
                && Objects.equals(basinArea, basin.basinArea)
                && Objects.equals(contributingArea, basin.contributingArea)
                && Objects.equals(parentBasinId, basin.parentBasinId)
                && Objects.equals(streams, basin.streams)
                && Objects.equals(subBasins, basin.subBasins);
    }

    @Override
    public int hashCode() {
        return Objects.hash(basinName, primaryStream, sortOrder, basinArea,
                contributingArea, parentBasinId, streams, subBasins);
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
        private LinkedList<Stream> streams = new LinkedList<>();
        private LinkedList<Stream> subBasins = new LinkedList<>();

        public Builder(String basinName, String officeId)
        {
            this.basinName = basinName;
            this.officeId = officeId;
        }

        public Builder withPrimaryStream(Stream primaryStream)
        {
            this.primaryStream = primaryStream;
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

        public Builder withParentBasinId(String parentBasinId)
        {
            this.parentBasinId = parentBasinId;
            return this;
        }

        public Builder withStream(Stream stream)
        {
            streams.add(stream);
            return this;
        }

        public Builder withSubBasin(Stream subBasin)
        {
            subBasins.add(subBasin);
            return this;
        }

        public Basin build()
        {
            return new Basin(this);
        }
    }

    @Override
    public void validate() throws FieldException {
        if (this.basinName == null) {
            throw new FieldException("Basin name field can't be null");
        }
        if (this.officeId == null) {
            throw new FieldException("Office Id field can't be null");
        }
    }
}
