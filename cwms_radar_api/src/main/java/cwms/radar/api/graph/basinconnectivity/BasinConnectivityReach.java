package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.ReachEdge;
import cwms.radar.api.graph.basinconnectivity.edges.StreamEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.data.dto.basinconnectivity.Stream;
import cwms.radar.data.dto.basinconnectivity.StreamReach;

import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityReach
{

    private final StreamReach _reach;
    private final BasinConnectivityStream _parentBasinConnStream;

    public BasinConnectivityReach(StreamReach reach, BasinConnectivityStream parentBasinConnStream)
    {
        _reach = reach;
        _parentBasinConnStream = parentBasinConnStream;
    }

    public List<ReachEdge> createReachEdges(List<StreamEdge> allStreamEdges)
    {
        List<ReachEdge> retval = new ArrayList<>();
        Stream parentStream = _parentBasinConnStream.getStream();
        BasinConnectivityNode firstNode = BasinConnectivityNode.getNode(_parentBasinConnStream, _reach.getUpstreamLocationId());
        BasinConnectivityNode lastNode = BasinConnectivityNode.getNode(_parentBasinConnStream, _reach.getDownstreamLocationId());
        if(parentStream.startsDownstream())
        {
            BasinConnectivityNode tempNode = firstNode;
            firstNode = lastNode;
            lastNode = tempNode;
        }
        StreamEdge firstStreamEdge = _parentBasinConnStream.getStreamEdgeWithSource(allStreamEdges, firstNode);
        ReachEdge reachEdge = new ReachEdge(_reach.getReachId(), _reach.getStreamId(), firstStreamEdge.getSource(), firstStreamEdge.getTarget());
        retval.add(reachEdge);
        while(!reachEdge.getTarget().equals(lastNode))
        {
            StreamEdge nextStreamEdge = _parentBasinConnStream.getStreamEdgeWithSource(allStreamEdges, reachEdge.getTarget());
            reachEdge = new ReachEdge(_reach.getReachId(), _reach.getStreamId(), nextStreamEdge.getSource(), nextStreamEdge.getTarget());
            retval.add(reachEdge);
        }
        return retval;
    }
}

