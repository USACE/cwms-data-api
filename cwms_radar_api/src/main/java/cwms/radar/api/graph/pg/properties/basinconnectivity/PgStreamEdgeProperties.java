package cwms.radar.api.graph.pg.properties.basinconnectivity;

import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgStreamEdgeProperties implements PgProperties
{
    private final String[] streamId;

    public PgStreamEdgeProperties(String streamId)
    {
        this.streamId = new String[]{streamId};
    }

    public String[] getStreamId()
    {
        return streamId;
    }
}
