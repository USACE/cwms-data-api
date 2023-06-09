package cwms.cda.api.graph.pg.properties.basinconnectivity;

import cwms.cda.api.graph.pg.properties.PgProperties;

public class PgStreamEdgeProperties implements PgProperties {
    private final String[] streamName;

    public PgStreamEdgeProperties(String streamName) {
        this.streamName = new String[]{streamName};
    }

    public String[] getStreamName() {
        return streamName;
    }
}
