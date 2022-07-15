package cwms.radar.api.graph.pg.properties.basinconnectivity;

import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgStreamNodeProperties implements PgProperties {
    private final String[] streamName;
    private final Double[] station;
    private final String[] bank;

    public PgStreamNodeProperties(String streamName, Double station, String bank) {
        this.streamName = new String[]{streamName};
        this.station = new Double[]{station};
        this.bank = new String[]{bank};
    }

    public String[] getStreamName() {
        return streamName;
    }

    public Double[] getStation() {
        return station;
    }

    public String[] getBank() {
        return bank;
    }
}
