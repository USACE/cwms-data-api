package cwms.radar.formatters.json;

import cwms.radar.api.graph.basinconnectivity.BasinConnectivityGraph;
import cwms.radar.api.graph.pgjson.PgJsonElement;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import java.util.List;

public final class PgJsonFormatter
{

    private PgJsonFormatter()
    {
        super();
    }

    public static String formatBasinConnectivityGraph(BasinConnectivityGraph graph)
    {
        String retVal = getDefaultPGJSON();
        if(graph != null)
        {
            retVal = Json.createObjectBuilder()
                    .add("nodes", getJsonArray(graph.getNodes()))
                    .add("edges", getJsonArray(graph.getEdges()))
                    .build()
                    .toString();
        }
        return retVal;
    }

    private static String getDefaultPGJSON()
    {
        return Json.createObjectBuilder()
                .add("nodes", Json.createArrayBuilder())
                .add("edges", Json.createArrayBuilder())
                .build()
                .toString();
    }

    private static JsonArray getJsonArray(List<? extends PgJsonElement> elements)
    {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for(PgJsonElement element : elements)
        {
            builder.add(element.toPGJSON());
        }
        return builder.build();
    }

}
