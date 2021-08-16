package cwms.radar.data.dto;

public class RecentValue implements CwmsDTO
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
}
