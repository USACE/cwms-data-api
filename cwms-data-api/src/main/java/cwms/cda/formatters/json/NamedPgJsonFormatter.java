package cwms.cda.formatters.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.graph.Graph;
import cwms.cda.api.graph.basinconnectivity.BasinConnectivityGraph;
import cwms.cda.api.graph.pg.dto.NamedPgGraphData;
import cwms.cda.api.graph.pg.dto.PgGraphData;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;

import java.util.ArrayList;
import java.util.List;

import static cwms.cda.formatters.Formats.NAMED_PGJSON;

public class NamedPgJsonFormatter implements OutputFormatter {
    private final ObjectMapper om;

    public NamedPgJsonFormatter()  {
        om = new ObjectMapper();
    }

    @Override
    public String getContentType() {
        return NAMED_PGJSON;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retVal;
        try {
            if (dto instanceof Basin) {
                Basin basin = (Basin) dto;
                Graph graph = new BasinConnectivityGraph.Builder(basin).build();
                String name = basin.getBasinName();
                retVal = formatNamedGraph(name, graph);
            } else {
                throw new FormattingException(dto.getClass().getSimpleName() + " is not currently supported for Named-PG-JSON format.");
            }
        } catch (JsonProcessingException e) {
            throw new FormattingException(e.getMessage());
        }
        return retVal;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        StringBuilder retVal = new StringBuilder();
        for(CwmsDTOBase dto : dtoList) {
            retVal.append(format(dto));
        }
        return retVal.toString();
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException("Unable to process your request. Deserialization of "
                + getContentType() + " not yet supported.");
    }

    private String formatNamedGraph(String name, Graph graph) throws JsonProcessingException {
        String retVal = getDefaultNamedPgJson(name);
        if (!graph.isEmpty()) {
            PgJsonFormatter pgJsonFormatter = new PgJsonFormatter();
            PgGraphData pgGraph = pgJsonFormatter.getFormattedGraph(graph);
            NamedPgGraphData namedGraphData = new NamedPgGraphData(name, pgGraph);
            retVal = om.writeValueAsString(namedGraphData);
        }
        return retVal;
    }

    private String getDefaultNamedPgJson(String name) throws JsonProcessingException {
        PgGraphData emptyGraphData = new PgGraphData(new ArrayList<>(), new ArrayList<>());
        return om.writeValueAsString(new NamedPgGraphData(name, emptyGraphData));
    }
}
