package cwms.cda.formatters.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.graph.Edge;
import cwms.cda.api.graph.Graph;
import cwms.cda.api.graph.Node;
import cwms.cda.api.graph.basinconnectivity.BasinConnectivityGraph;
import cwms.cda.api.graph.basinconnectivity.edges.ReachEdge;
import cwms.cda.api.graph.basinconnectivity.edges.StreamEdge;
import cwms.cda.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.cda.api.graph.pg.dto.*;
import cwms.cda.api.graph.pg.properties.PgProperties;
import cwms.cda.api.graph.pg.properties.basinconnectivity.PgReachEdgeProperties;
import cwms.cda.api.graph.pg.properties.basinconnectivity.PgStreamEdgeProperties;
import cwms.cda.api.graph.pg.properties.basinconnectivity.PgStreamNodeProperties;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import service.annotations.FormatService;

import java.util.*;

@FormatService(contentType = Formats.PGJSON, dataTypes = {Basin.class})
public final class PgJsonFormatter implements OutputFormatter {

    private final ObjectMapper om;

    public PgJsonFormatter() {
        om = new ObjectMapper();
    }

    private String formatGraph(Graph graph) throws JsonProcessingException {
        String retVal = getDefaultPGJSON();
        if (!graph.isEmpty()) {
            retVal = om.writeValueAsString(getFormattedGraph(graph));
        }
        return retVal;
    }

    public PgGraphData getFormattedGraph(Graph graph) {
        List<PgNodeData> formattedNodes = formatNodes(graph.getNodes());
        List<PgEdgeData> formattedEdges = formatEdges(graph.getEdges());
        return new PgGraphData(formattedNodes, formattedEdges);
    }

    private List<PgEdgeData> formatEdges(List<Edge> edges) {
        List<PgEdgeData> retVal = new ArrayList<>();
        for (Edge edge : edges) {
            PgEdgeData edgeData;
            if (edge instanceof StreamEdge) {
                StreamEdge streamEdge = (StreamEdge) edge;
                PgProperties properties = new PgStreamEdgeProperties(streamEdge.getStreamId());
                String[] labels = new String[]{streamEdge.getLabel()};
                edgeData = new PgEdgeData(streamEdge.getSource().getId(), streamEdge.getTarget().getId(), labels, false, properties);
            } else if(edge instanceof  ReachEdge) {
                ReachEdge reachEdge = (ReachEdge) edge;
                PgProperties properties = new PgReachEdgeProperties(reachEdge.getStreamId(), reachEdge.getId());
                String[] labels = new String[]{reachEdge.getLabel()};
                edgeData = new PgEdgeData(reachEdge.getSource().getId(), reachEdge.getTarget().getId(), labels, false, properties);
            } else {
                throw new IllegalArgumentException("PG-JSON format does not currently support this Edge type");
            }
            retVal.add(edgeData);
        }
        return retVal;
    }

    private List<PgNodeData> formatNodes(List<Node> nodes) {
        List<PgNodeData> retVal = new ArrayList<>();
        for (Node node : nodes) {
            if (node instanceof BasinConnectivityNode) {
                String nodeId = node.getId();
                BasinConnectivityNode basinConnNode = (BasinConnectivityNode) node;
                String[] labels = new String[]{basinConnNode.getLabel()};
                PgProperties properties = new PgStreamNodeProperties(basinConnNode.getStreamId(), basinConnNode.getStation(), basinConnNode.getBank());
                retVal.add(new PgNodeData(nodeId, labels, properties));
            } else {
                throw new IllegalArgumentException("PG-JSON format does not currently support this Node type");
            }
        }
        return retVal;
    }

    private String getDefaultPGJSON() throws JsonProcessingException {
        return om.writeValueAsString(new PgGraphData(new ArrayList<>(), new ArrayList<>()));
    }

    @Override
    public String getContentType() {
        return Formats.PGJSON;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retVal;
        Graph graph;
        if (dto instanceof Basin) {
            Basin basin = (Basin) dto;
            graph = new BasinConnectivityGraph.Builder(basin).build();
        } else {
            throw new FormattingException(dto.getClass().getSimpleName() + " is not currently supported for PG-JSON format.");
        }

        try {
            retVal = formatGraph(graph);
        } catch (JsonProcessingException e) {
            throw new FormattingException(e.getMessage());
        }
        return retVal;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        StringBuilder retVal = new StringBuilder();
        for (CwmsDTOBase dto : dtoList) {
            retVal.append(format(dto));
        }
        return retVal.toString();
    }
}
