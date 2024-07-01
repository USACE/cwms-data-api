package cwms.cda.api.graph.basinconnectivity;

import cwms.cda.api.graph.basinconnectivity.edges.ReachEdge;
import cwms.cda.api.graph.basinconnectivity.edges.StreamEdge;
import cwms.cda.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.cda.data.dto.basinconnectivity.Stream;
import cwms.cda.data.dto.basinconnectivity.StreamReach;
import java.util.ArrayList;
import java.util.List;

public class BasinConnectivityReach {

    private final StreamReach reach;
    private final BasinConnectivityStream parentBasinConnStream;

    public BasinConnectivityReach(StreamReach reach,
                                  BasinConnectivityStream parentBasinConnStream) {
        this.reach = reach;
        this.parentBasinConnStream = parentBasinConnStream;
    }

    public List<ReachEdge> createReachEdges(List<StreamEdge> allStreamEdges) {
        List<ReachEdge> retval = new ArrayList<>();
        Stream parentStream = parentBasinConnStream.getStream();
        BasinConnectivityNode firstNode = BasinConnectivityNode.getNode(parentBasinConnStream,
                reach.getUpstreamLocationName());
        BasinConnectivityNode lastNode = BasinConnectivityNode.getNode(parentBasinConnStream,
                reach.getDownstreamLocationName());
        if (parentStream.startsDownstream()) {
            BasinConnectivityNode tempNode = firstNode;
            firstNode = lastNode;
            lastNode = tempNode;
        }
        StreamEdge firstStreamEdge = parentBasinConnStream.getStreamEdgeWithSource(allStreamEdges,
                firstNode);
        ReachEdge reachEdge = new ReachEdge(reach.getReachName(), reach.getStreamName(),
                firstStreamEdge.getSource(), firstStreamEdge.getTarget());
        retval.add(reachEdge);
        while (!reachEdge.getTarget().equals(lastNode)) {
            StreamEdge nextStreamEdge =
                    parentBasinConnStream.getStreamEdgeWithSource(allStreamEdges,
                            reachEdge.getTarget());
            reachEdge = new ReachEdge(reach.getReachName(), reach.getStreamName(),
                    nextStreamEdge.getSource(), nextStreamEdge.getTarget());
            retval.add(reachEdge);
        }
        return retval;
    }
}
