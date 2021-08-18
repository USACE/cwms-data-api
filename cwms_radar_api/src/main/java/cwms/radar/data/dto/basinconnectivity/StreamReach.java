package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.basinconnectivity.graph.BasinConnectivityNode;
import cwms.radar.data.dto.basinconnectivity.graph.ReachEdge;
import cwms.radar.data.dto.basinconnectivity.graph.StreamEdge;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

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

    List<ReachEdge> createReachEdges(List<StreamEdge> streamEdges, Stream parentStream, StreamReach reach)
    {
        List<ReachEdge> retval = new ArrayList<>();
        BasinConnectivityNode firstNode = getNode(parentStream, reach.getUpstreamLocationId());
        BasinConnectivityNode lastNode = getNode(parentStream, reach.getDownstreamLocationId());
        if(parentStream.startsDownstream())
        {
            BasinConnectivityNode tempNode = firstNode;
            firstNode = lastNode;
            lastNode = tempNode;
        }
        StreamEdge firstStreamEdge = parentStream.getStreamEdgeWithSource(streamEdges, firstNode);
        ReachEdge reachEdge = new ReachEdge(reach.getReachId(), reach.getStreamId(), firstStreamEdge.getSource(), firstStreamEdge.getTarget());
        retval.add(reachEdge);
        while(!reachEdge.getTarget().equals(lastNode))
        {
            StreamEdge nextStreamEdge = parentStream.getStreamEdgeWithSource(streamEdges, reachEdge.getTarget());
            reachEdge = new ReachEdge(reach.getReachId(), reach.getStreamId(), nextStreamEdge.getSource(), nextStreamEdge.getTarget());
            retval.add(reachEdge);
        }
        return retval;
    }

    private BasinConnectivityNode getNode(Stream parentStream, @NotNull String name)
    {
        BasinConnectivityNode retval = null;
        List<BasinConnectivityNode> streamNodes = parentStream.getStreamNodes();
        for (BasinConnectivityNode node : streamNodes)
        {
            if (name.equalsIgnoreCase(node.getName()))
            {
                retval = node;
                break;
            }
        }
        return retval;
    }

}
