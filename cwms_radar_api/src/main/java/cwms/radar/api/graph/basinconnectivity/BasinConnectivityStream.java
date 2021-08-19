package cwms.radar.api.graph.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.edges.BasinConnectivityEdge;
import cwms.radar.api.graph.basinconnectivity.edges.ReachEdge;
import cwms.radar.api.graph.basinconnectivity.edges.StreamEdge;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNode;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityNodeComparator;
import cwms.radar.api.graph.basinconnectivity.nodes.BasinConnectivityStreamLocation;
import cwms.radar.api.graph.basinconnectivity.nodes.EmptyStreamNode;
import cwms.radar.data.dto.basinconnectivity.Stream;
import cwms.radar.data.dto.basinconnectivity.StreamLocation;
import cwms.radar.data.dto.basinconnectivity.StreamReach;

import java.util.*;

public class BasinConnectivityStream
{
    private final List<BasinConnectivityNode> _streamNodes;
    private final Stream _stream;
    private final String _receivingStreamId;
    private final List<BasinConnectivityStream> _tributaries;
    private final List<BasinConnectivityReach> _reaches;
    private BasinConnectivityNode _diversionNode;

    public BasinConnectivityStream(Stream stream, String receivingStreamId)
    {
        _stream = stream;
        _tributaries = buildBasinConnectivityTributaries();
        _reaches = buildBasinConnectivityReaches();
        _receivingStreamId= receivingStreamId;
        _streamNodes = new ArrayList<>(buildStreamLocationNodes(stream.getStreamLocations()));
        addEmptyNodes(); //adds terminal nodes and tributary junction nodes to _streamNodes, then sorts by station
    }

    private List<BasinConnectivityStream> buildBasinConnectivityTributaries()
    {
        List<BasinConnectivityStream> retval = new ArrayList<>();
        Stream stream = getStream();
        for(Stream tributary : stream.getTributaries())
        {
            retval.add(new BasinConnectivityStream(tributary, stream.getStreamId()));

        }
        return retval;
    }

    private List<BasinConnectivityReach> buildBasinConnectivityReaches()
    {
        List<BasinConnectivityReach> retval = new ArrayList<>();
        Stream stream = getStream();
        for(StreamReach reach : stream.getStreamReaches())
        {
            retval.add(new BasinConnectivityReach(reach, this));
        }
        return retval;
    }

    private List<BasinConnectivityStreamLocation> buildStreamLocationNodes(List<StreamLocation> streamLocations)
    {
        List<BasinConnectivityStreamLocation> retval = new ArrayList<>();
        for(StreamLocation streamLocation : streamLocations)
        {
            retval.add(new BasinConnectivityStreamLocation(streamLocation));
        }
        return retval;
    }

    public Stream getStream()
    {
        return _stream;
    }

    public List<BasinConnectivityNode> getStreamNodes()
    {
        return new ArrayList<>(_streamNodes);
    }

    private boolean isPrimary()
    {
        return _receivingStreamId == null;
    }

    public List<BasinConnectivityStream> getTributaries()
    {
        return _tributaries;
    }

    public List<BasinConnectivityReach> getReaches()
    {
        return _reaches;
    }

    private static List<StreamEdge> createAllStreamEdges(BasinConnectivityStream stream)
    {
        return createAllStreamEdgesRecursive(stream, null);
    }

    private static List<StreamEdge> createAllStreamEdgesRecursive(BasinConnectivityStream currentBasinConnStream, BasinConnectivityStream receivingStream)
    {
        List<StreamEdge> retval = new ArrayList<>();
        List<BasinConnectivityNode> streamNodes = currentBasinConnStream.getStreamNodes();
        BasinConnectivityNode diversionNode = currentBasinConnStream.getDiversionNode();
        BasinConnectivityNode confluenceNode = null;
        Stream currentStream = currentBasinConnStream.getStream();
        boolean isTributary = !currentBasinConnStream.isPrimary();
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
        for(BasinConnectivityStream tributary : currentBasinConnStream.getTributaries())
        {
            retval.addAll(createAllStreamEdgesRecursive(tributary, currentBasinConnStream));
        }
        return retval;
    }

    public StreamEdge getStreamEdgeWithSource(List<StreamEdge> edges, BasinConnectivityNode node)
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

    private void sortStreamNodesByStation()
    {
        _streamNodes.sort(new BasinConnectivityNodeComparator(getStream().startsDownstream()));
    }

    private BasinConnectivityNode getFirstNode()
    {
        BasinConnectivityNode retval = null;
        if(!_streamNodes.isEmpty())
        {
            ArrayList<BasinConnectivityNode> copyNodes = new ArrayList<>(_streamNodes);
            copyNodes.sort(new BasinConnectivityNodeComparator(getStream().startsDownstream()));
            retval = copyNodes.get(0);
        }
        return retval;

    }

    private BasinConnectivityNode getDiversionNode()
    {
        return _diversionNode;
    }

    public List<BasinConnectivityEdge> getEdges()
    {
        List<BasinConnectivityEdge> retval = new ArrayList<>();
        List<StreamEdge> streamEdges = createAllStreamEdges(this);
        retval.addAll(streamEdges);
        retval.addAll(buildReachEdges(streamEdges, this));
        return retval;
    }

    public List<BasinConnectivityNode> getNodes()
    {
        List<BasinConnectivityNode> retval = new ArrayList<>();
        addAllNodes(retval);
        return retval;
    }

    private void addAllNodes(List<BasinConnectivityNode> retval)
    {
        retval.addAll(getStreamNodes());
        for(BasinConnectivityStream tributary: getTributaries())
        {
            tributary.addAllNodes(retval);
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
        Stream stream = getStream();
        boolean startsDownstream = stream.startsDownstream();
        double firstStation = startsDownstream ? stream.getStreamLength() : 0.0;
        double lastStation = startsDownstream ? 0.0 : stream.getStreamLength();
        BasinConnectivityNode firstNode = getNodeAtStation(firstStation);
        BasinConnectivityNode lastNode = getNodeAtStation(lastStation);
        if(firstNode == null)
        {
            firstNode = new EmptyStreamNode(stream.getStreamId(), firstStation, "L");
            _streamNodes.add(firstNode);
        }
        if(lastNode == null && isPrimary()) //if this is a tributary (not primary), 'last node' will be junction node on primary stream
        {
            lastNode = new EmptyStreamNode(stream.getStreamId(), lastStation, "L");
            _streamNodes.add(lastNode);
        }
    }

    private void addStreamJunctionNodes()
    {
        for(BasinConnectivityStream basinConnTributary : getTributaries())
        {
            Stream tributary = basinConnTributary.getStream();
            Double confluenceStation = tributary.getConfluenceStation();
            Double diversionStation = tributary.getDiversionStation();
            BasinConnectivityStream divertsFromStream = getChildStream(tributary.getDivertingStreamId());
            if(divertsFromStream != null) //if the stream the tributary diverts from is also in basin
            {
                BasinConnectivityNode diversionNode = divertsFromStream.getNodeAtStation(diversionStation);
                if(diversionNode == null) //if no stream node at this station, we must make an empty node
                {
                    diversionNode = new EmptyStreamNode(divertsFromStream.getStream().getStreamId(), diversionStation, basinConnTributary.getStream().getDiversionBank());
                    divertsFromStream.insertNode(diversionNode);
                    basinConnTributary.removeNode(basinConnTributary.getFirstNode()); //remove first node from tributary, because this node now exists on tributary's diversion stream
                }
                basinConnTributary.setDiversionNode(diversionNode);
            }
            BasinConnectivityNode existingNodeAtConfluenceStation = getNodeAtStation(confluenceStation);
            if(existingNodeAtConfluenceStation == null) //if no stream node at this station, we must make an empty node
            {
                EmptyStreamNode emptyNode = new EmptyStreamNode(getStream().getStreamId(), confluenceStation, basinConnTributary.getStream().getConfluenceBank());
                _streamNodes.add(emptyNode);
            }
        }
    }

    private List<ReachEdge> buildReachEdges(List<StreamEdge> allStreamEdges, BasinConnectivityStream parentBasinConnStream)
    {
        List<ReachEdge> retval = new ArrayList<>();
        addAllReachEdgesRecursive(retval, allStreamEdges, parentBasinConnStream);
        return retval;
    }

    private void addAllReachEdgesRecursive(List<ReachEdge> reachEdges, List<StreamEdge> allStreamEdges, BasinConnectivityStream parentBasinConnStream)
    {
        for(BasinConnectivityReach basinConnReach : parentBasinConnStream.getReaches())
        {
            reachEdges.addAll(basinConnReach.createReachEdges(allStreamEdges));
        }
        for(BasinConnectivityStream tributary : parentBasinConnStream.getTributaries())
        {
            addAllReachEdgesRecursive(reachEdges, allStreamEdges, tributary);
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
        int index = Collections.binarySearch(_streamNodes, node, new BasinConnectivityNodeComparator(getStream().startsDownstream()));
        if (index < 0)
        {
            index = -index - 1;
        }
        _streamNodes.add(index, node);
    }

    /**
     *
     * @param streamId - input-stream id of Stream object we are retrieving if input-stream has flow into this stream
     * @return returns Stream object for streamId if input-stream has water flowing into this stream somewhere down the line (ie. a tributary, or a tributary's tributary, etc),
     *         otherwise, return null.
     */
    private BasinConnectivityStream getChildStream(String streamId)
    {
        BasinConnectivityStream retval = null;
        List<BasinConnectivityStream> tributaries = getTributaries();
        for(BasinConnectivityStream tributary : tributaries)
        {
            if(tributary.getStream().getStreamId().equalsIgnoreCase(streamId))
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
}
