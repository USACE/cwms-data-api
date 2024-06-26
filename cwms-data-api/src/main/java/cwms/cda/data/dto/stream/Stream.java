package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Stream.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Stream implements CwmsDTOBase {

    private final Boolean startsDownstream;
    private final StreamNode flowsIntoStreamNode;
    private final StreamNode divertsFromStreamNode;
    private final Double length;
    private final Double averageSlope;
    private final String lengthUnits;
    private final String slopeUnits;
    private final String comment;
    private final CwmsId id;

    private Stream(Builder builder) {
        this.startsDownstream = builder.startsDownstream;
        this.flowsIntoStreamNode = builder.flowsIntoStreamNode;
        this.divertsFromStreamNode = builder.divertsFromStreamNode;
        this.length = builder.length;
        this.averageSlope = builder.averageSlope;
        this.lengthUnits = builder.lengthUnits;
        this.slopeUnits = builder.slopeUnits;
        this.comment = builder.comment;
        this.id = builder.id;
    }

    @Override
    public void validate() throws FieldException {
        if (this.id == null) {
            throw new FieldException("The 'id' field of a Stream cannot be null.");
        }
        id.validate();
    }

    public Boolean getStartsDownstream() {
        return startsDownstream;
    }

    @JsonIgnore
    public String getOfficeId() {
        return id.getOfficeId();
    }

    public StreamNode getFlowsIntoStreamNode() {
        return flowsIntoStreamNode;
    }

    public StreamNode getDivertsFromStreamNode() {
        return divertsFromStreamNode;
    }

    public Double getLength() {
        return length;
    }

    public Double getAverageSlope() {
        return averageSlope;
    }

    public String getLengthUnits() {
        return lengthUnits;
    }

    public String getSlopeUnits() {
        return slopeUnits;
    }

    public String getComment() {
        return comment;
    }

    public CwmsId getId() {
        return id;
    }

    public static final class Builder {
        private Boolean startsDownstream;
        private StreamNode flowsIntoStreamNode;
        private StreamNode divertsFromStreamNode;
        private Double length;
        private Double averageSlope;
        private String lengthUnits;
        private String slopeUnits;
        private String comment;
        private CwmsId id;

        public Builder withStartsDownstream(Boolean startsDownstream) {
            this.startsDownstream = startsDownstream;
            return this;
        }

        public Builder withFlowsIntoStreamNode(StreamNode flowsIntoStreamNode) {
            this.flowsIntoStreamNode = flowsIntoStreamNode;
            return this;
        }

        public Builder withDivertsFromStreamNode(StreamNode divertsFromStreamNode) {
            this.divertsFromStreamNode = divertsFromStreamNode;
            return this;
        }

        public Builder withLength(Double length) {
            this.length = length;
            return this;
        }

        public Builder withAverageSlope(Double averageSlope) {
            this.averageSlope = averageSlope;
            return this;
        }

        public Builder withLengthUnits(String lengthUnits) {
            this.lengthUnits = lengthUnits;
            return this;
        }

        public Builder withSlopeUnits(String slopeUnits) {
            this.slopeUnits = slopeUnits;
            return this;
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withId(CwmsId id) {
            this.id = id;
            return this;
        }

        public Stream build() {
            return new Stream(this);
        }
    }
}