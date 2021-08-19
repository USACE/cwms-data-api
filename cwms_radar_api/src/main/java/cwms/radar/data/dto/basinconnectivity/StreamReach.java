package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

public class StreamReach implements CwmsDTO
{
    private final String _upstreamLocationId;
    private final String _downstreamLocationId;
    private final String _streamId;
    private final String _reachId;

    public StreamReach(String reachId, String streamId, String upstreamLocationId, String downstreamLocationId)
    {
        _streamId = streamId;
        _reachId = reachId;
        _upstreamLocationId = upstreamLocationId;
        _downstreamLocationId = downstreamLocationId;
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

}
