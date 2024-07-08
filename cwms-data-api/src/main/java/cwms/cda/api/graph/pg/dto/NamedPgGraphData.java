package cwms.cda.api.graph.pg.dto;

public class NamedPgGraphData {
    private final String name;
    private final PgGraphData graph;

    public NamedPgGraphData(String name, PgGraphData graphData) {
        this.name = name;
        this.graph = graphData;
    }

    public String getName() {
        return name;
    }

    public PgGraphData getGraph() {
        return graph;
    }
}
