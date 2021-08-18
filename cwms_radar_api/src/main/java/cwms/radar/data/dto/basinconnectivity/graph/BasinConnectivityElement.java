package cwms.radar.data.dto.basinconnectivity.graph;

import javax.json.JsonObject;

interface BasinConnectivityElement
{
    String getLabel();
    JsonObject getProperties();
    JsonObject toPGJSON();
}
