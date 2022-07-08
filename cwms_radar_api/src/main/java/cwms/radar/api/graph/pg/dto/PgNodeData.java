package cwms.radar.api.graph.pg.dto;

import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgNodeData {
    private final String name;
    private final String[] labels;
    private final PgProperties properties;

    public PgNodeData(String name, String[] labels, PgProperties properties) {
        this.name = name;
        this.labels = labels;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public String[] getLabels() {
        return labels;
    }

    public PgProperties getProperties() {
        return properties;
    }
}
