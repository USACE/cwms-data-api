package cwms.radar.data.dto;

public class Basin implements CwmsDTO
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

}
