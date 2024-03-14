package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.Formats;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
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
