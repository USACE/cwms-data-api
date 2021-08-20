package cwms.radar.data.dto.basinconnectivity;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.basinconnectivity.buildercontracts.Build;

public class BasinBuilder implements Build<Basin>
{

    private final String _basinId;
    private final String _officeId;
    private Stream _primaryStream;
    private Double _sortOrder;
    private Double _basinArea;
    private Double _contributingArea;
    private String _parentBasinId;

    public BasinBuilder(Basin basin)
    {
        _basinId = basin.getBasinId();
        _officeId = basin.getOfficeId();
        _primaryStream = basin.getPrimaryStream();
    }

    public BasinBuilder(String basinId, String officeId)
    {
        _basinId = basinId;
        _officeId = officeId;
    }

    public BasinBuilder withSortOrder(Double sortOrder)
    {
        _sortOrder = sortOrder;
        return this;
    }

    public BasinBuilder withBasinArea(Double basinArea)
    {
        _basinArea = basinArea;
        return this;
    }

    public BasinBuilder withContributingArea(Double contributingArea)
    {
        _contributingArea = contributingArea;
        return this;
    }

    public BasinBuilder withParentBasinId(String parentBasinId)
    {
        _parentBasinId = parentBasinId;
        return this;
    }

    public BasinBuilder withPrimaryStream(Stream primaryStream)
    {
        _primaryStream = primaryStream;
        return this;
    }

    String getBasinId()
    {
        return _basinId;
    }

    Stream getPrimaryStream()
    {
        return _primaryStream;
    }

    String getOfficeId()
    {
        return _officeId;
    }

    Double getSortOrder()
    {
        return _sortOrder;
    }

    Double getBasinArea()
    {
        return _basinArea;
    }

    Double getContributingArea()
    {
        return _contributingArea;
    }

    String getParentBasinId()
    {
        return _parentBasinId;
    }

    @Override
    public Basin build()
    {
        return new Basin(this);
    }
}
