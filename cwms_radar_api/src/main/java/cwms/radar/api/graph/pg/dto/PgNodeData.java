package cwms.radar.api.graph.pg.dto;

import cwms.radar.api.graph.pg.properties.PgProperties;

public class PgNodeData
{
    private final String id;
    private final String[] labels;
    private final PgProperties properties;

    public PgNodeData(String id, String[] labels, PgProperties properties)
    {
        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

    public String getId()
    {
        return id;
    }

    public String[] getLabels()
    {
        return labels;
    }

    public PgProperties getProperties()
    {
        return properties;
    }
}
