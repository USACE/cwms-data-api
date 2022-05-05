package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTO;

public class StreamReach implements CwmsDTO
{
    private final String upstreamLocationName;
    private final String downstreamLocationName;
    private final String streamName;
    private final String reachName;
    private final String officeId;
    private final String comment;
    private final String configuration;

    StreamReach(Builder builder)
    {
        streamName = builder.streamName;
        reachName = builder.reachName;
        upstreamLocationName = builder.upstreamLocationName;
        downstreamLocationName = builder.downstreamLocationName;
        comment = builder.comment;
        configuration = builder.configuration;
        officeId = builder.officeId;
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

    public String getOfficeId()
    {
        return officeId;
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

    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }

}
