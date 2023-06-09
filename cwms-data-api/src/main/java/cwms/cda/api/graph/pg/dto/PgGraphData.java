package cwms.cda.api.graph.pg.dto;

import java.util.List;

public class PgGraphData {

    private final List<PgNodeData> nodes;
    private final List<PgEdgeData> edges;

    public PgGraphData(List<PgNodeData> nodeData, List<PgEdgeData> edgeData) {
        this.nodes = nodeData;
        this.edges = edgeData;
    }

    public List<PgNodeData> getNodes() {
        return nodes;
    }

    public List<PgEdgeData> getEdges() {
        return edges;
    }
}
