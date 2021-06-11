package cwms.radar.data.dto;

public class AssignedLocation implements CwmsDTO
{
	private String locationId;
	private String baseLocationId;
	private String subLocationId;
	private String aliasId;
	private Number attribute;
	private Number locationCode;
	private String refLocationId;

	public AssignedLocation(String locationId, String baseLocationId, String subLocationId, String aliasId,
							Number attribute, Number locationCode, String refLocationId)
	{
		this.locationId = locationId;
		this.baseLocationId = baseLocationId;
		this.subLocationId = subLocationId;
		this.aliasId = aliasId;
		this.attribute = attribute;
		this.locationCode = locationCode;
		this.refLocationId = refLocationId;
	}

	public String getLocationId()
	{
		return locationId;
	}

	public String getBaseLocationId()
	{
		return baseLocationId;
	}

	public String getSubLocationId()
	{
		return subLocationId;
	}

	public String getAliasId()
	{
		return aliasId;
	}

	public Number getAttribute()
	{
		return attribute;
	}

	public Number getLocationCode()
	{
		return locationCode;
	}

	public String getRefLocationId()
	{
		return refLocationId;
	}
}
