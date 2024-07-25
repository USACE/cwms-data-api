package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = StreamLocationNode.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamLocationNode extends CwmsDTOBase {

    @JsonProperty(required = true)
    private final CwmsId id; //stream location id
    @JsonProperty(required = true)
    private final StreamNode streamNode;

    private StreamLocationNode(Builder builder) {
        this.id = builder.id;
        this.streamNode = builder.streamNode;
    }

    public CwmsId getId() {
        return id;
    }

    public StreamNode getStreamNode() {
        return streamNode;
    }

    public static class Builder {
        private CwmsId id;
        private StreamNode streamNode;

        public Builder withId(CwmsId streamLocationId) {
            this.id = streamLocationId;
            return this;
        }

        public Builder withStreamNode(StreamNode streamNode) {
            this.streamNode = streamNode;
            return this;
        }

        public StreamLocationNode build() {
            return new StreamLocationNode(this);
        }
    }
}
