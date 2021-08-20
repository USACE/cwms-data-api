package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.basinconnectivity.buildercontracts.Build;

public class StreamReachBuilder implements Build<StreamReach>
{

    private final String _streamId;
    private final String _reachId;
    private final String _upstreamLocationId;
    private final String _downstreamLocationId;
    private final String _officeId;
    private String _comment;
    private String _configuration;

    public StreamReachBuilder(String reachId, String streamId, String upstreamLocationId, String downstreamLocationId, String officeId)
    {
        _streamId = streamId;
        _reachId = reachId;
        _upstreamLocationId = upstreamLocationId;
        _downstreamLocationId = downstreamLocationId;
        _officeId = officeId;
    }

    public StreamReachBuilder(StreamReach streamReach)
    {
        _streamId = streamReach.getStreamId();
        _reachId = streamReach.getReachId();
        _upstreamLocationId = streamReach.getUpstreamLocationId();
        _downstreamLocationId = streamReach.getDownstreamLocationId();
        _officeId = streamReach.getOfficeId();
        _comment = streamReach.getComment();
        _configuration = streamReach.getConfiguration();
    }

    public StreamReachBuilder withComment(String comment)
    {
        _comment = comment;
        return this;
    }

    public StreamReachBuilder withConfiguration(String configuration)
    {
        _configuration = configuration;
        return this;
    }

    String getReachId()
    {
        return _reachId;
    }

    String getStreamId()
    {
        return _streamId; //stream that reach is on
    }

    String getUpstreamLocationId()
    {
        return _upstreamLocationId;
    }

    String getDownstreamLocationId()
    {
        return _downstreamLocationId;
    }

    String getComment()
    {
        return _comment;
    }

    String getConfiguration()
    {
        return _configuration;
    }

    String getOfficeId()
    {
        return _officeId;
    }

    @Override
    public StreamReach build()
    {
        return new StreamReach(this);
    }
}
