package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.api.graph.basinconnectivity.BasinConnectivityGraphBuilder;
import cwms.radar.api.graph.pgjson.PgJsonGraph;
import cwms.radar.data.dto.PgJsonDTO;

public final class Basin implements PgJsonDTO
{
    private final String _basinId;
    private final String _officeId;
    private final Stream _primaryStream;
    private final Double _sortOrder;
    private final Double _basinArea;
    private final Double _contributingArea;
    private final String _parentBasinId;

    Basin(BasinBuilder basinBuilder)
    {
        _basinId = basinBuilder.getBasinId();
        _primaryStream = basinBuilder.getPrimaryStream();
        _officeId = basinBuilder.getOfficeId();
        _sortOrder = basinBuilder.getSortOrder();
        _basinArea = basinBuilder.getBasinArea();
        _contributingArea = basinBuilder.getContributingArea();
        _parentBasinId = basinBuilder.getParentBasinId();
    }

    public String getBasinId()
    {
        return _basinId;
    }

    public Stream getPrimaryStream()
    {
        return _primaryStream;
    }

    public String getOfficeId()
    {
        return _officeId;
    }

    public Double getSortOrder()
    {
        return _sortOrder;
    }

    public Double getBasinArea()
    {
        return _basinArea;
    }

    public Double getContributingArea()
    {
        return _contributingArea;
    }

    public String getParentBasinId()
    {
        return _parentBasinId;
    }

    @Override
    public PgJsonGraph getPgJsonGraph()
    {
        return new BasinConnectivityGraphBuilder(this).build();
    }
}
