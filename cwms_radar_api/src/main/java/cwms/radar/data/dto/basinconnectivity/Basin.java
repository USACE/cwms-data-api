package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;

import javax.json.Json;
import javax.json.JsonObject;

public final class Basin implements CwmsDTO
{
    private final String _basinId;
    private final Stream _primaryStream;

    public Basin(String basinId, Stream primaryStream)
    {
        _basinId = basinId;
        _primaryStream = primaryStream;
    }

    public String getBasinId()
    {
        return _basinId;
    }

    public Stream getPrimaryStream()
    {
        return _primaryStream;
    }

    public String toPGJSONString()
    {
        return toPGJSON().toString();
    }

    private JsonObject toPGJSON()
    {
        JsonObject retval = getDefaultPGJSON();
        Stream primaryStream = getPrimaryStream();
        if(primaryStream != null)
        {
            retval = primaryStream.toPGJSON();
        }
        return retval;
    }

    private JsonObject getDefaultPGJSON()
    {
        return Json.createObjectBuilder()
                .add("nodes", Json.createArrayBuilder())
                .add("edges", Json.createArrayBuilder())
                .build();
    }

}
