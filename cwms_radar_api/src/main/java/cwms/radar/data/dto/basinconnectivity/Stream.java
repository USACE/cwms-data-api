package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.basinconnectivity.graph.*;

import javax.json.*;
import java.util.*;

public final class Stream implements CwmsDTO
{
    private final String _streamId;
    private final List<Stream> _tributaries;//streams that flow into this
    private final List<BasinConnectivityNode> _streamNodes; //streamLocations on stream
    private final List<StreamReach> _streamReaches;
    private final boolean _startsDownstream;
    private final String _divertingStreamId; //flows from
    private final String _receivingStreamId; //flows into
    private final String _confluenceBank;
    private final String _diversionBank;
    private final Double _streamLength;
    private final Double _confluenceStation;
    private final Double _diversionStation;
    private BasinConnectivityNode _diversionNode;

    //receiving stream should be NULL if this is primary stream of basin
    //This could probably be refactored to use builder pattern
    public Stream(String streamId, boolean startsDownstream, String divertingStreamId, String receivingStreamId,
                  String diversionBank, String confluenceBank, Double diversionStation, Double confluenceStation,
                  Double streamLength, Set<StreamLocation> streamLocations, Set<Stream> tributaries, Set<StreamReach> reachesOnStream)
    {
        _streamId = streamId;
        _startsDownstream = startsDownstream;
        _divertingStreamId = divertingStreamId;
        _receivingStreamId = receivingStreamId;
        _confluenceBank = confluenceBank;
        _diversionBank = diversionBank;
        _streamLength = streamLength;
        _confluenceStation = confluenceStation;
        _diversionStation = diversionStation;
        _streamNodes = new ArrayList<>(streamLocations);
        _tributaries = new ArrayList<>(tributaries);
        _streamReaches = new ArrayList<>(reachesOnStream);
        addEmptyNodes(); //adds terminal nodes and tributary junction nodes to _streamNodes, then sorts by station
    }

    public List<BasinConnectivityNode> getStreamNodes()
    {
        return new ArrayList<>(_streamNodes);
    }

    public List<StreamReach> getStreamReaches()
    {
        return new ArrayList<>(_streamReaches);
    }

    public List<Stream> getTributaries()
    {
        return new ArrayList<>(_tributaries);
    }

    public String getStreamId()
    {
        return _streamId;
    }

    public String getDivertingStreamId()
    {
        return _divertingStreamId;
    }

    public String getReceivingStreamId()
    {
        return _receivingStreamId;
    }

    public boolean startsDownstream()
    {
        return _startsDownstream;
    }

    public String getDiversionBank()
    {
        return _diversionBank;
    }

    public String getConfluenceBank()
    {
        return _confluenceBank;
    }

    public Double getStreamLength()
    {
        return _streamLength;
    }

    private boolean isPrimary()
    {
        return _receivingStreamId == null;
    }

    public Double getConfluenceStation()
    {
        return _confluenceStation;
    }

    public Double getDiversionStation()
    {
        return _diversionStation;
    }

    private BasinConnectivityNode getDiversionNode()
    {
        return _diversionNode;
    }

    JsonObject toPGJSON()
    {
        return Json.createObjectBuilder()
                .add("nodes", getNodes())
                .add("edges", getEdges())
                .build();
    }

    private JsonArray getNodes()
    {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        addStreamNodesToJSON(this, arrayBuilder);
        return arrayBuilder.build();
    }

    private JsonArray getEdges()
    {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        List<StreamEdge> streamEdges = createAllStreamEdges(this);
        List<ReachEdge> reachEdges = createAllReachEdges(streamEdges, this);
        addEdgesToJSON(streamEdges, arrayBuilder);
        addEdgesToJSON(reachEdges, arrayBuilder);
        return arrayBuilder.build();
    }

    private void addStreamNodesToJSON(Stream stream, JsonArrayBuilder builder)
    {
        for(BasinConnectivityNode node : stream.getStreamNodes())
        {
            builder.add(node.toPGJSON());
        }
        for(Stream tributary : stream.getTributaries())
        {
            addStreamNodesToJSON(tributary, builder);
        }
    }

    private void addEdgesToJSON(List<? extends BasinConnectivityEdge> edges, JsonArrayBuilder builder)
    {
        for(BasinConnectivityEdge edge : edges)
        {
            builder.add(edge.toPGJSON());
        }
    }

    private void addEmptyNodes()
    {
        addTerminalNodes();
        addStreamJunctionNodes();
        sortStreamNodesByStation();
    }

    private void addTerminalNodes()
    {
        boolean startsDownstream = startsDownstream();
        double firstStation = startsDownstream ? getStreamLength() : 0.0;
        double lastStation = startsDownstream ? 0.0 : getStreamLength();
        BasinConnectivityNode firstNode = getNodeAtStation(firstStation);
        BasinConnectivityNode lastNode = getNodeAtStation(lastStation);
        if(firstNode == null)
        {
            firstNode = new EmptyStreamNode(getStreamId(), firstStation, "L");
            _streamNodes.add(firstNode);
        }
        if(lastNode == null && isPrimary()) //if this is a tributary (not primary), 'last node' will be junction node on primary stream
        {
            lastNode = new EmptyStreamNode(getStreamId(), lastStation, "L");
            _streamNodes.add(lastNode);
        }
    }

    private void addStreamJunctionNodes()
    {
        for(Stream tributary : getTributaries())
        {
            Double confluenceStation = tributary.getConfluenceStation();
            Double diversionStation = tributary.getDiversionStation();
            Stream divertsFromStream = getChildStream(tributary.getDivertingStreamId());
            if(divertsFromStream != null) //if the stream the tributary diverts from is also in basin
            {
                BasinConnectivityNode diversionNode = divertsFromStream.getNodeAtStation(diversionStation);
                if(diversionNode == null) //if no stream node at this station, we must make an empty node
                {
                    diversionNode = new EmptyStreamNode(divertsFromStream.getStreamId(), diversionStation, tributary.getDiversionBank());
                    divertsFromStream.insertNode(diversionNode);
                    tributary.removeNode(tributary.getFirstNode()); //remove first node from tributary, because this node now exists on tributary's diversion stream
                }
                tributary.setDiversionNode(diversionNode);
            }
            BasinConnectivityNode existingNodeAtConfluenceStation = getNodeAtStation(confluenceStation);
            if(existingNodeAtConfluenceStation == null) //if no stream node at this station, we must make an empty node
            {
                EmptyStreamNode emptyNode = new EmptyStreamNode(getStreamId(), confluenceStation, tributary.getConfluenceBank());
                _streamNodes.add(emptyNode);
            }
        }
    }

    private void setDiversionNode(BasinConnectivityNode diversionNode)
    {
        _diversionNode = diversionNode;
    }

    private void removeNode(BasinConnectivityNode node)
    {
        _streamNodes.remove(node);
    }

    private void insertNode(BasinConnectivityNode node)
    {
        int index = Collections.binarySearch(_streamNodes, node, new StreamNodeComparator(startsDownstream()));
        if (index < 0)
        {
            index = -index - 1;
        }
        _streamNodes.add(index, node);
    }

    private List<StreamEdge> createAllStreamEdges(Stream stream)
    {
        return createAllStreamEdgesRecursive(stream, null);
    }

    private List<StreamEdge> createAllStreamEdgesRecursive(Stream currentStream, Stream receivingStream)
    {
        List<StreamEdge> retval = new ArrayList<>();
        List<BasinConnectivityNode> streamNodes = currentStream.getStreamNodes();
        BasinConnectivityNode diversionNode = currentStream.getDiversionNode();
        BasinConnectivityNode confluenceNode = null;
        boolean isTributary = !currentStream.isPrimary();
        if(isTributary) //initialize confluence node if this is a tributary
        {
            confluenceNode = receivingStream.getNodeAtStation(currentStream.getConfluenceStation());
        }
        if(diversionNode != null) //if stream diverts from another stream in basin, add edge between that stream and first node in this stream
        {
            BasinConnectivityNode targetNode = confluenceNode; //for case where there are no nodes on stream, but stream connects nodes on 2 different streams
            if(!streamNodes.isEmpty()) //if there are nodes on stream, then set target of first edge (coming from diversion stream) to the first node on stream
            {
                targetNode = streamNodes.get(0);
            }
            StreamEdge edge = new StreamEdge(currentStream.getStreamId(), diversionNode, targetNode);
            retval.add(edge);
        }
        for(int i=0; i < streamNodes.size() -1; i++)
        {
            StreamEdge edge = new StreamEdge(currentStream.getStreamId(), streamNodes.get(i), streamNodes.get(i+1));
            retval.add(edge);
        }
        if(isTributary && !streamNodes.isEmpty()) //if tributary, then connect last edge to junction node in receiving stream
        {
            StreamEdge edge = new StreamEdge(currentStream.getStreamId(), streamNodes.get(streamNodes.size() -1), confluenceNode);
            retval.add(edge);
        }
        for(Stream tributary : currentStream.getTributaries())
        {
            retval.addAll(createAllStreamEdgesRecursive(tributary, currentStream));
        }
        return retval;
    }

    private List<ReachEdge> createAllReachEdges(List<StreamEdge> allStreamEdges, Stream parentStream)
    {
        List<ReachEdge> retval = new ArrayList<>();
        addAllReachEdgesRecursive(retval, allStreamEdges, parentStream);
        return retval;
    }

    private void addAllReachEdgesRecursive(List<ReachEdge> reachEdges, List<StreamEdge> allStreamEdges, Stream parentStream)
    {
        List<StreamReach> reaches = parentStream.getStreamReaches();
        for(StreamReach reach : reaches)
        {
            reachEdges.addAll(reach.createReachEdges(allStreamEdges, parentStream, reach));
        }
        List<Stream> tributaries = parentStream.getTributaries();
        for(Stream tributary : tributaries)
        {
            addAllReachEdgesRecursive(reachEdges, allStreamEdges, tributary);
        }

    }

    StreamEdge getStreamEdgeWithSource(List<StreamEdge> edges, BasinConnectivityNode node)
    {
        StreamEdge retval = null;
        for(StreamEdge edge : edges)
        {
            if(node.equals(edge.getSource()))
            {
                retval = edge;
                break;
            }
        }
        return retval;
    }

    private BasinConnectivityNode getNodeAtStation(Double station)
    {
        BasinConnectivityNode retval = null;
        if(station != null)
        {
            for (BasinConnectivityNode node : getStreamNodes())
            {
                if (node.getStation().doubleValue() == station.doubleValue())
                {
                    retval = node;
                    break;
                }
            }
        }
        return retval;
    }

    /**
     *
     * @param streamId - input-stream id of Stream object we are retrieving if input-stream has flow into this stream
     * @return returns Stream object for streamId if input-stream has water flowing into this stream somewhere down the line (ie. a tributary, or a tributary's tributary, etc),
     *         otherwise, return null.
     */
    private Stream getChildStream(String streamId)
    {
        Stream retval = null;
        List<Stream> tributaries = getTributaries();
        for(Stream tributary : tributaries)
        {
            if(tributary.getStreamId().equalsIgnoreCase(streamId))
            {
                retval = tributary;
            }
            if(retval == null)
            {
                retval = tributary.getChildStream(streamId);
            }
            if(retval != null)
            {
                break;
            }
        }
        return retval;
    }

    private void sortStreamNodesByStation()
    {
        _streamNodes.sort(new StreamNodeComparator(startsDownstream()));
    }

    private BasinConnectivityNode getFirstNode()
    {
        BasinConnectivityNode retval = null;
        if(!_streamNodes.isEmpty())
        {
            ArrayList<BasinConnectivityNode> copyNodes = new ArrayList<>(_streamNodes);
            copyNodes.sort(new StreamNodeComparator(startsDownstream()));
            retval = copyNodes.get(0);
        }
        return retval;

    }
}

