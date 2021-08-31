package cwms.radar.api.graph.pg.properties.basinconnectivity;


import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgStreamNodeProperties implements PgProperties
{
    private final String[] streamId;
    private final Double[] station;
    private final String[] bank;

    public PgStreamNodeProperties(String streamId, Double station, String bank)
    {
        this.streamId = new String[]{streamId};
        this.station = new Double[]{station};
        this.bank = new String[]{bank};
    }

    public String[] getStreamId()
    {
        return streamId;
    }

    public Double[] getStation()
    {
        return station;
    }

    public String[] getBank()
    {
        return bank;
    }
}
