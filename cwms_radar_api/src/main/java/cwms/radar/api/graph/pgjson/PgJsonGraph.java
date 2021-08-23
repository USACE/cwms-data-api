package cwms.radar.api.graph.pgjson;

import java.util.List;

public interface PgJsonGraph
{
    List<PgJsonEdge> getEdges();
    List<PgJsonNode> getNodes();
    String getId();
    default boolean isEmpty()
    {
        return getEdges().isEmpty() && getNodes().isEmpty();
    }
}
