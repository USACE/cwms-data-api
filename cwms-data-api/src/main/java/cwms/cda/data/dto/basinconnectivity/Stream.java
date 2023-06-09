package cwms.cda.data.dto.basinconnectivity;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.basinconnectivity.buildercontracts.stream.*;

import java.util.*;

public final class Stream extends CwmsDTO
{
    private final String streamName;
    private final List<Stream> tributaries;//streams that flow into this
    private final List<StreamReach> streamReaches;
    private final boolean startsDownstream;
    private final String divertingStreamId; //flows from
    private final String receivingStreamId; //flows into
    private final String confluenceBank;
    private final String diversionBank;
    private final Double streamLength;
    private final Double confluenceStation;
    private final Double diversionStation;
    private final List<StreamLocation> streamLocations;
    private final String comment;
    private final Double averageSlope;

    private Stream(Builder streamBuilder)
    {
        super(streamBuilder.officeId);
        streamName = streamBuilder.streamName;
        startsDownstream = streamBuilder.startsDownstream;
        divertingStreamId = streamBuilder.divertingStreamId;
        receivingStreamId = streamBuilder.receivingStreamId;
        confluenceBank = streamBuilder.confluenceBank;
        diversionBank = streamBuilder.diversionBank;
        streamLength = streamBuilder.streamLength;
        confluenceStation = streamBuilder.confluenceStation;
        diversionStation = streamBuilder.diversionStation;
        streamLocations = streamBuilder.streamLocations;
        tributaries = streamBuilder.tributaries;
        streamReaches = streamBuilder.streamReaches;
        comment = streamBuilder.comment;
        averageSlope = streamBuilder.averageSlope;
    }

    public List<StreamLocation> getStreamLocations()
    {
        return new ArrayList<>(streamLocations);
    }

    public List<StreamReach> getStreamReaches()
    {
        return new ArrayList<>(streamReaches);
    }

    public List<Stream> getTributaries()
    {
        return new ArrayList<>(tributaries);
    }

    public String getStreamName()
    {
        return streamName;
    }

    public String getDivertingStreamId()
    {
        return divertingStreamId;
    }

    public String getReceivingStreamId()
    {
        return receivingStreamId;
    }

    public boolean startsDownstream()
    {
        return startsDownstream;
    }

    public String getDiversionBank()
    {
        return diversionBank;
    }

    public String getConfluenceBank()
    {
        return confluenceBank;
    }

    public Double getStreamLength()
    {
        return streamLength;
    }

    public Double getConfluenceStation()
    {
        return confluenceStation;
    }

    public Double getDiversionStation()
    {
        return diversionStation;
    }

    public String getComment()
    {
        return comment;
    }

    public Double getAverageSlope()
    {
        return averageSlope;
    }

    public static class Builder implements BuildStream, BuildDiversionStation, BuildConfluenceStation, BuildDiversionBank, BuildConfluenceBank
    {
        private final String officeId;
        private final String streamName;
        private final boolean startsDownstream;
        private String divertingStreamId; //flows from
        private String receivingStreamId; //flows into
        private String confluenceBank;
        private String diversionBank;
        private final Double streamLength;
        private Double confluenceStation;
        private Double diversionStation;
        private final List<StreamLocation> streamLocations = new ArrayList<>();
        private final List<Stream> tributaries = new ArrayList<>();//streams that flow into this
        private final List<StreamReach> streamReaches = new ArrayList<>();
        private String comment;
        private Double averageSlope;

        public Builder(String streamName, boolean startsDownstream, Double streamLength, String officeId) {
            this.streamName = streamName;
            this.startsDownstream = startsDownstream;
            this.streamLength = streamLength;
            this.officeId = officeId;
        }

        public Builder(Stream stream) {
            this.streamName = stream.getStreamName();
            this.startsDownstream = stream.startsDownstream();
            this.streamLength = stream.getStreamLength();
            this.officeId = stream.getOfficeId();
            this.averageSlope = stream.getAverageSlope();
            this.comment = stream.getComment();
            this.receivingStreamId = stream.getReceivingStreamId();
            this.confluenceStation = stream.getConfluenceStation();
            this.confluenceBank = stream.getConfluenceBank();
            this.divertingStreamId = stream.getDivertingStreamId();
            this.diversionStation = stream.getDiversionStation();
            this.diversionBank = stream.getDiversionBank();
            this.tributaries.addAll(stream.getTributaries());
            this.streamReaches.addAll(stream.getStreamReaches());
            this.streamLocations.addAll(stream.getStreamLocations());
        }

        public BuildDiversionStation withDivertingStreamId(String divertingStreamId) {
            this.divertingStreamId = divertingStreamId;
            return this;
        }

        public BuildConfluenceStation withReceivingStreamId(String receivingStreamId) {
            this.receivingStreamId = receivingStreamId;
            return this;
        }

        @Override
        public BuildStream withDiversionBank(String diversionBank) {
            this.diversionBank = diversionBank;
            return this;
        }

        @Override
        public BuildStream withConfluenceBank(String confluenceBank) {
            this.confluenceBank = confluenceBank;
            return this;
        }

        @Override
        public BuildConfluenceBank withConfluenceStation(Double confluenceStation) {
            this.confluenceStation = confluenceStation;
            return this;
        }

        @Override
        public BuildDiversionBank withDiversionStation(Double diversionStation) {
            this.diversionStation = diversionStation;
            return this;
        }

        @Override
        public Stream build() {
            return new Stream(this);
        }

        public BuildStream withStreamLocations(Collection<StreamLocation> streamLocations) {
            this.streamLocations.clear();
            this.streamLocations.addAll(streamLocations);
            return this;
        }

        public BuildStream withTributaries(Collection<Stream> tributaries) {
            this.tributaries.clear();
            this.tributaries.addAll(tributaries);
            return this;
        }

        public BuildStream withStreamReaches(Collection<StreamReach> streamReaches) {
            this.streamReaches.clear();
            this.streamReaches.addAll(streamReaches);
            return this;
        }

        public BuildStream withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public BuildStream withAverageSlope(Double averageSlope) {
            this.averageSlope = averageSlope;
            return this;
        }
    }

    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }

}
