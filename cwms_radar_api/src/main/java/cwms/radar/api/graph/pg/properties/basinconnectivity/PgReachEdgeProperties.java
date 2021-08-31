package cwms.radar.api.graph.pg.properties.basinconnectivity;

public class PgReachEdgeProperties extends PgStreamEdgeProperties
{
    private final String[] reachId;

    public PgReachEdgeProperties(String streamId, String reachId)
    {
        super(streamId);
        this.reachId = new String[]{reachId};
    }

    public String[] getReachId()
    {
        return reachId;
    }
}
