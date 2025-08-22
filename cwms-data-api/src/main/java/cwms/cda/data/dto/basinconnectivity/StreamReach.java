package cwms.cda.data.dto.basinconnectivity;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;

public class StreamReach extends CwmsDTO
{
    private final String upstreamLocationName;
    private final String downstreamLocationName;
    private final String streamName;
    private final String reachName;
    private final String comment;
    private final String configuration;

    StreamReach(Builder builder)
    {
        super(builder.officeId);
        streamName = builder.streamName;
        reachName = builder.reachName;
        upstreamLocationName = builder.upstreamLocationName;
        downstreamLocationName = builder.downstreamLocationName;
        comment = builder.comment;
        configuration = builder.configuration;
    }

    public String getReachName()
    {
        return reachName;
    }

    public String getStreamName()
    {
        return streamName; //stream that reach is on
    }

    public String getUpstreamLocationName()
    {
        return upstreamLocationName;
    }

    public String getDownstreamLocationName()
    {
        return downstreamLocationName;
    }

    public String getComment()
    {
        return comment;
    }

    public String getConfiguration()
    {
        return configuration;
    }

    public static class Builder
    {

        private final String streamName;
        private final String reachName;
        private final String upstreamLocationName;
        private final String downstreamLocationName;
        private final String officeId;
        private String comment;
        private String configuration;

        public Builder(String reachName, String streamName, String upstreamLocationName, String downstreamLocationName, String officeId) {
            this.streamName = streamName;
            this.reachName = reachName;
            this.upstreamLocationName = upstreamLocationName;
            this.downstreamLocationName = downstreamLocationName;
            this.officeId = officeId;
        }

        public Builder(StreamReach streamReach) {
            this.streamName = streamReach.getStreamName();
            this.reachName = streamReach.getReachName();
            this.upstreamLocationName = streamReach.getUpstreamLocationName();
            this.downstreamLocationName = streamReach.getDownstreamLocationName();
            this.officeId = streamReach.getOfficeId();
            this.comment = streamReach.getComment();
            this.configuration = streamReach.getConfiguration();
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withConfiguration(String configuration) {
            this.configuration = configuration;
            return this;
        }

        public StreamReach build() {
            return new StreamReach(this);
        }
    }
}
