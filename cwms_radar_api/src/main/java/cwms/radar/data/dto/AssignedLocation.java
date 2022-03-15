package cwms.radar.data.dto;

import cwms.radar.api.errors.FieldException;

public class AssignedLocation implements CwmsDTO
{
	private String locationId;
	private String officeId;
	private String aliasId;
	private Number attribute;

	private String refLocationId;

	public AssignedLocation(String locationId, String office, String aliasId,
							Number attribute, String refLocationId)
	{
		this.locationId = locationId;
		this.officeId = office;
		this.aliasId = aliasId;
		this.attribute = attribute;
		this.refLocationId = refLocationId;
	}

	public String getLocationId()
	{
		return locationId;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getAliasId()
	{
		return aliasId;
	}

	public Number getAttribute()
	{
		return attribute;
	}

	public String getRefLocationId()
	{
		return refLocationId;
	}

	@Override
	public void validate() throws FieldException {

	}
}
