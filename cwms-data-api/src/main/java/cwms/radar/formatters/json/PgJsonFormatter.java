package cwms.radar.formatters.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.radar.api.graph.Edge;
import cwms.radar.api.graph.Graph;
import cwms.radar.api.graph.Node;
import cwms.radar.api.graph.basinconnectivity.BasinConnectivityGraph;
import cwms.radar.api.graph.basinconnectivity.edges.ReachEdge;
import cwms.radar.api.graph.basinconnectivity.edges.StreamEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.api.graph.pg.dto.*;
import cwms.radar.api.graph.pg.properties.PgProperties;
import cwms.radar.api.graph.pg.properties.basinconnectivity.PgReachEdgeProperties;
import cwms.radar.api.graph.pg.properties.basinconnectivity.PgStreamEdgeProperties;
import cwms.radar.api.graph.pg.properties.basinconnectivity.PgStreamNodeProperties;
import cwms.radar.data.dto.CwmsDTOBase;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

import java.util.*;

@FormatService(contentType = Formats.PGJSON, dataTypes = {Basin.class})
public final class PgJsonFormatter implements OutputFormatter
{

    private final ObjectMapper om;

    public PgJsonFormatter()
    {
        om = new ObjectMapper();
    }

    private String formatGraph(Graph graph) throws JsonProcessingException
    {
        String retVal = getDefaultPGJSON();
        if(!graph.isEmpty())
        {
            retVal = om.writeValueAsString(getFormattedGraph(graph));
        }
        return retVal;
    }

    public PgGraphData getFormattedGraph(Graph graph)
    {
        List<PgNodeData> formattedNodes = formatNodes(graph.getNodes());
        List<PgEdgeData> formattedEdges = formatEdges(graph.getEdges());
        return new PgGraphData(formattedNodes, formattedEdges);
    }
    private List<PgEdgeData> formatEdges(List<Edge> edges)
    {
        List<PgEdgeData> retval = new ArrayList<>();
        for(Edge edge : edges)
        {
            PgEdgeData edgeData;
            if(edge instanceof StreamEdge)
            {
                StreamEdge streamEdge = (StreamEdge) edge;
                PgProperties properties = new PgStreamEdgeProperties(streamEdge.getStreamId());
                String[] labels = new String[]{streamEdge.getLabel()};
                edgeData = new PgEdgeData(streamEdge.getSource().getId(), streamEdge.getTarget().getId(), labels, false, properties);
            }
            else if(edge instanceof  ReachEdge)
            {
                ReachEdge reachEdge = (ReachEdge) edge;
                PgProperties properties = new PgReachEdgeProperties(reachEdge.getStreamId(), reachEdge.getId());
                String[] labels = new String[]{reachEdge.getLabel()};
                edgeData = new PgEdgeData(reachEdge.getSource().getId(), reachEdge.getTarget().getId(), labels, false, properties);
            }
            else
            {
                throw new IllegalArgumentException("PG-JSON format does not currently support this Edge type");
            }
            retval.add(edgeData);
        }
        return retval;
    }

    private List<PgNodeData> formatNodes(List<Node> nodes)
    {
        List<PgNodeData> retval = new ArrayList<>();
        for(Node node : nodes)
        {
            if(node instanceof BasinConnectivityNode)
            {
                String nodeId = node.getId();
                BasinConnectivityNode basinConnNode = (BasinConnectivityNode) node;
                String[] labels = new String[]{basinConnNode.getLabel()};
                PgProperties properties = new PgStreamNodeProperties(basinConnNode.getStreamId(), basinConnNode.getStation(), basinConnNode.getBank());
                retval.add(new PgNodeData(nodeId, labels, properties));
            }
            else
            {
                throw new IllegalArgumentException("PG-JSON format does not currently support this Node type");
            }
        }
        return retval;
    }

    private String getDefaultPGJSON() throws JsonProcessingException
    {
        return om.writeValueAsString(new PgGraphData(new ArrayList<>(), new ArrayList<>()));
    }

    @Override
    public String getContentType()
    {
        return Formats.PGJSON;
    }

    @Override
    public String format(CwmsDTOBase dto)
    {
        String retVal;
        Graph graph;
        if(dto instanceof Basin)
        {
            Basin basin = (Basin) dto;
            graph = new BasinConnectivityGraph.Builder(basin).build();
        }
        else
        {
            throw new FormattingException(dto.getClass().getSimpleName() + " is not currently supported for PG-JSON format.");
        }
        try
        {
            retVal = formatGraph(graph);
        }
        catch (JsonProcessingException e)
        {
            throw new FormattingException(e.getMessage());
        }
        return retVal;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList)
    {
        StringBuilder retval = new StringBuilder();
        for(CwmsDTOBase dto : dtoList)
        {
            retval.append(format(dto));
        }
        return retval.toString();
    }
}
