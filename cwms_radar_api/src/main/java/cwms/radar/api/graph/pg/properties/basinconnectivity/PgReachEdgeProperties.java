package cwms.radar.api.graph.pg.properties.basinconnectivity;

public class PgReachEdgeProperties extends PgStreamEdgeProperties {
    private final String[] reachName;

    public PgReachEdgeProperties(String streamName, String reachName) {
        super(streamName);
        this.reachName = new String[]{reachName};
    }

    public String[] getReachName() {
        return reachName;
    }
}
