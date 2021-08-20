package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

public class StreamReach implements CwmsDTO
{
    private final String _upstreamLocationId;
    private final String _downstreamLocationId;
    private final String _streamId;
    private final String _reachId;
    private final String _officeId;
    private final String _comment;
    private final String _configuration;

    StreamReach(StreamReachBuilder builder)
    {
        _streamId = builder.getStreamId();
        _reachId = builder.getReachId();
        _upstreamLocationId = builder.getUpstreamLocationId();
        _downstreamLocationId = builder.getDownstreamLocationId();
        _comment = builder.getComment();
        _configuration = builder.getConfiguration();
        _officeId = builder.getOfficeId();
    }

    public String getReachId()
    {
        return _reachId;
    }

    public String getStreamId()
    {
        return _streamId; //stream that reach is on
    }

    public String getUpstreamLocationId()
    {
        return _upstreamLocationId;
    }

    public String getDownstreamLocationId()
    {
        return _downstreamLocationId;
    }

    public String getComment()
    {
        return _comment;
    }

    public String getConfiguration()
    {
        return _configuration;
    }

    public String getOfficeId()
    {
        return _officeId;
    }

}
