package cwms.radar.formatters.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.graph.Graph;
import cwms.radar.api.graph.basinconnectivity.BasinConnectivityGraph;
import cwms.radar.api.graph.pg.dto.NamedPgGraphData;
import cwms.radar.api.graph.pg.dto.PgGraphData;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

import java.util.ArrayList;
import java.util.List;

@FormatService(contentType = Formats.NAMED_PGJSON, dataTypes = {Basin.class})
public class NamedPgJsonFormatter implements OutputFormatter
{
    private final ObjectMapper om;

    public NamedPgJsonFormatter()
    {
        om = new ObjectMapper();
    }

    @Override
    public String getContentType()
    {
        return Formats.NAMED_PGJSON;
    }

    @Override
    public String format(CwmsDTO dto)
    {
        String retVal;
        try
        {
            if(dto instanceof Basin)
            {
                Basin basin = (Basin) dto;
                Graph graph = new BasinConnectivityGraph.Builder(basin).build();
                String name = basin.getBasinName();
                retVal = formatNamedGraph(name, graph);
            }
            else
            {
                throw new FormattingException(dto.getClass().getSimpleName() + " is not currently supported for Basin-PG-JSON format.");
            }
        }
        catch (JsonProcessingException e)
        {
            throw new FormattingException(e.getMessage());
        }
        return retVal;
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

    private String formatNamedGraph(String name, Graph graph) throws JsonProcessingException
    {
        String retVal = getDefaultNamedPgJson(name);
        if(!graph.isEmpty())
        {
            PgJsonFormatter pgJsonFormatter = new PgJsonFormatter();
            PgGraphData pgGraph = pgJsonFormatter.getFormattedGraph(graph);
            NamedPgGraphData namedGraphData = new NamedPgGraphData(name, pgGraph);
            retVal = om.writeValueAsString(namedGraphData);
        }
        return retVal;
    }

    private String getDefaultNamedPgJson(String name) throws JsonProcessingException
    {
        PgGraphData emptyGraphData = new PgGraphData(new ArrayList<>(), new ArrayList<>());
        return om.writeValueAsString(new NamedPgGraphData(name, emptyGraphData));
    }
}
