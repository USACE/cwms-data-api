package cwms.radar.api.graph.pgjson;

import javax.json.JsonObject;

public interface PgJsonElement
{
    String getLabel();
    JsonObject getProperties();
    JsonObject toPGJSON();
}
