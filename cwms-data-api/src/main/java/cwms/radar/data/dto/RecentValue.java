package cwms.radar.data.dto;

import cwms.radar.api.errors.FieldException;

public class RecentValue implements CwmsDTOBase
{
	String id;
	TsvDqu dqu;

	public RecentValue(String id, TsvDqu dqu)
	{
		this.id = id;
		this.dqu = dqu;
	}

	public String getId()
	{
		return id;
	}

	public TsvDqu getDqu()
	{
		return dqu;
	}

	@Override
	public void validate() throws FieldException {
		// TODO Auto-generated method stub

	}
}
