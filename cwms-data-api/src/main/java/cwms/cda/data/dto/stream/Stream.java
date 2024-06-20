package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = Stream.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Stream implements CwmsDTOBase {

    private final Boolean startsDownstream;
    private final StreamJunctionIdentifier flowsIntoStream;
    private final StreamJunctionIdentifier divertsFromStream;
    private final Double length;
    private final Double slope;
    private final String comment;
    private final LocationIdentifier streamId;

    private Stream(Builder builder) {
        this.startsDownstream = builder.startsDownstream;
        this.flowsIntoStream = builder.flowsIntoStream;
        this.divertsFromStream = builder.divertsFromStream;
        this.length = builder.length;
        this.slope = builder.slope;
        this.comment = builder.comment;
        this.streamId = builder.streamId;
    }

    @Override
    public void validate() throws FieldException {
        if (this.streamId == null) {
            throw new FieldException("The 'locationIdentifier' field of a Stream cannot be null.");
        }
        streamId.validate();
    }

    public Boolean getStartsDownstream() {
        return startsDownstream;
    }

    @JsonIgnore
    public String getOfficeId() {
        return streamId.getOfficeId();
    }

    public StreamJunctionIdentifier getFlowsIntoStream() {
        return flowsIntoStream;
    }

    public StreamJunctionIdentifier getDivertsFromStream() {
        return divertsFromStream;
    }

    public Double getLength() {
        return length;
    }

    public Double getSlope() {
        return slope;
    }

    public String getComment() {
        return comment;
    }

    public LocationIdentifier getStreamId() {
        return streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Stream that = (Stream) o;
        return Objects.equals(getStartsDownstream(), that.getStartsDownstream())
                && Objects.equals(getFlowsIntoStream(), that.getFlowsIntoStream())
                && Objects.equals(getDivertsFromStream(), that.getDivertsFromStream())
                && Objects.equals(getLength(), that.getLength())
                && Objects.equals(getSlope(), that.getSlope())
                && Objects.equals(getComment(), that.getComment())
                && Objects.equals(getStreamId(), that.getStreamId());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getStartsDownstream());
        result = 31 * result + Objects.hashCode(getFlowsIntoStream());
        result = 31 * result + Objects.hashCode(getDivertsFromStream());
        result = 31 * result + Objects.hashCode(getLength());
        result = 31 * result + Objects.hashCode(getSlope());
        result = 31 * result + Objects.hashCode(getComment());
        result = 31 * result + Objects.hashCode(getStreamId());
        return result;
    }

    public static final class Builder {
        private Boolean startsDownstream;
        private StreamJunctionIdentifier flowsIntoStream;
        private StreamJunctionIdentifier divertsFromStream;
        private Double length;
        private Double slope;
        private String comment;
        private LocationIdentifier streamId;

        public Builder withStartsDownstream(Boolean startsDownstream) {
            this.startsDownstream = startsDownstream;
            return this;
        }

        public Builder withFlowsIntoStream(StreamJunctionIdentifier flowsIntoStream) {
            this.flowsIntoStream = flowsIntoStream;
            return this;
        }

        public Builder withDivertsFromStream(StreamJunctionIdentifier divertsFromStream) {
            this.divertsFromStream = divertsFromStream;
            return this;
        }

        public Builder withLength(Double length) {
            this.length = length;
            return this;
        }

        public Builder withSlope(Double slope) {
            this.slope = slope;
            return this;
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withStreamId(LocationIdentifier streamId) {
            this.streamId = streamId;
            return this;
        }

        public Stream build() {
            return new Stream(this);
        }
    }
}