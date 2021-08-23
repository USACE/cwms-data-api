package cwms.radar.formatters.json;

import cwms.radar.api.graph.pgjson.PgJsonElement;
import cwms.radar.api.graph.pgjson.PgJsonGraph;
import cwms.radar.data.dto.*;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.util.List;

@FormatService(contentType = Formats.JSON, dataTypes = {Basin.class})
public final class PgJsonFormatter implements OutputFormatter
{

    public PgJsonFormatter()
    {
        super();
    }

    private String formatGraph(PgJsonGraph graph)
    {
        String id = graph.getId();
        String retVal = getDefaultPGJSON(id);
        if(!graph.isEmpty())
        {
            JsonObject nodesEdgesJson = Json.createObjectBuilder()
                    .add("nodes", getJsonArray(graph.getNodes()))
                    .add("edges", getJsonArray(graph.getEdges()))
                    .build();
            retVal = Json.createObjectBuilder()
                    .add(id, nodesEdgesJson)
                    .build()
                    .toString();
        }
        return retVal;
    }

    private static String getDefaultPGJSON(String id)
    {
        JsonObject nodesEdgesJson = Json.createObjectBuilder()
                .add("nodes", Json.createArrayBuilder())
                .add("edges", Json.createArrayBuilder()).build();
        return Json.createObjectBuilder()
                .add(id, nodesEdgesJson)
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

    @Override
    public String getContentType()
    {
        return Formats.JSON;
    }

    @Override
    public String format(CwmsDTO dto)
    {
        if(!(dto instanceof PgJsonDTO))
        {
            throw new FormattingException(dto.getClass().getSimpleName() + " is not a pg-json data object. Object must implement PgJsonDTO.");
        }
        PgJsonDTO pgJsonDTO = (PgJsonDTO) dto;
        PgJsonGraph graph = pgJsonDTO.getPgJsonGraph();
        return formatGraph(graph);
    }

    @Override
    public String format(List<? extends CwmsDTO> dtoList)
    {
        StringBuilder retval = new StringBuilder();
        for(CwmsDTO dto : dtoList)
        {
            retval.append(format(dto));
        }
        return retval.toString();
    }
}
