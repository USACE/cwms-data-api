package cwms.radar.api.graph.pg.properties.basinconnectivity;

import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgStreamEdgeProperties implements PgProperties {
    private final String[] streamName;

    public PgStreamEdgeProperties(String streamName) {
        this.streamName = new String[]{streamName};
    }

    public String[] getStreamName() {
        return streamName;
    }
}
