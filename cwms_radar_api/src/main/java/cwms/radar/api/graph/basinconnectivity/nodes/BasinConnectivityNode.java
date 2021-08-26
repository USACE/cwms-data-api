package cwms.radar.api.graph.basinconnectivity.nodes;

import cwms.radar.api.graph.basinconnectivity.BasinConnectivityStream;
import cwms.radar.api.graph.pgjson.PgJsonNode;
import org.jetbrains.annotations.NotNull;

import javax.json.Json;
import javax.json.JsonObject;
import java.util.List;

public abstract class BasinConnectivityNode implements PgJsonNode
{
    private final String _streamId;
    private final Double _station;
    private final String _bank;

    protected BasinConnectivityNode(String streamId, Double station, String bank)
    {
        _streamId = streamId;
        _station = station;
        if(bank == null)
        {
            bank = "L"; //default bank
        }
        _bank = bank;
    }

    public abstract String getId();

    public String getBank()
    {
        return _bank;
    }

    public Double getStation()
    {
        return _station;
    }

    public String getStreamId()
    {
        return _streamId;
    }

    @Override
    public JsonObject getProperties()
    {
        return Json.createObjectBuilder()
                .add("stream_id", Json.createArrayBuilder().add(getStreamId()))
                .add("station", Json.createArrayBuilder().add(getStation()))
                .add("bank", Json.createArrayBuilder().add(getBank()))
                .build();
    }

    @Override
    public JsonObject toPGJSON()
    {
        return Json.createObjectBuilder()
                .add("id", getId())
                .add("labels", Json.createArrayBuilder().add(getLabel()))
                .add("properties", getProperties())
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean retval = false;
        if(obj instanceof BasinConnectivityNode)
        {
            BasinConnectivityNode other = (BasinConnectivityNode) obj;
            retval = other.getId() != null && other.getStreamId() != null && other.getStation() != null;
            if(retval)
            {
                retval = other.getId().equalsIgnoreCase(getId())
                        && other.getStation().equals(getStation())
                        && other.getStreamId().equalsIgnoreCase(getStreamId());
            }
        }
        return retval;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        String name = getId();
        Double station = getStation();
        String streamId = getStreamId();
        result = prime * result + ((name == null) ? 0 : name.toLowerCase().hashCode());
        result = prime * result + ((station == null) ? 0 : station.hashCode());
        result = prime * result + ((streamId == null) ? 0 : streamId.toLowerCase().hashCode());
        return result;
    }

    public static BasinConnectivityNode getNode(BasinConnectivityStream parentStream, @NotNull String name)
    {
        BasinConnectivityNode retval = null;
        List<BasinConnectivityNode> streamNodes = parentStream.getStreamNodes();
        for (BasinConnectivityNode node : streamNodes)
        {
            if (name.equalsIgnoreCase(node.getId()))
            {
                retval = node;
                break;
            }
        }
        return retval;
    }
}
