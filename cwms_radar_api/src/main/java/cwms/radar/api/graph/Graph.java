package cwms.radar.api.graph;

import java.util.List;

public interface Graph
{
    List<Edge> getEdges();
    List<Node> getNodes();
    String getId();
    default boolean isEmpty()
    {
        return getEdges().isEmpty() && getNodes().isEmpty();
    }
}
