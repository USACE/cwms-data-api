package cwms.cda.data.dto;

import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.Formats;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class RecentValue extends CwmsDTOBase
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
