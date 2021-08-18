package cwms.radar.data.dto.basinconnectivity.graph;

import javax.json.Json;
import javax.json.JsonObject;

public abstract class BasinConnectivityNode implements BasinConnectivityElement
{
    private final String _streamId;
    private final Double _station;
    private final String _bank;

    protected BasinConnectivityNode(String streamId, Double station, String bank)
    {
        _streamId = streamId;
        _station = station;
        _bank = bank;
    }

    public abstract String getName();

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
                .add("id", getName())
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
            retval = other.getName() != null && other.getStreamId() != null && other.getStation() != null;
            if(retval)
            {
                retval = other.getName().equalsIgnoreCase(getName())
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
        String name = getName();
        Double station = getStation();
        String streamId = getStreamId();
        result = prime * result + ((name == null) ? 0 : name.toLowerCase().hashCode());
        result = prime * result + ((station == null) ? 0 : station.hashCode());
        result = prime * result + ((streamId == null) ? 0 : streamId.toLowerCase().hashCode());
        return result;
    }
}
