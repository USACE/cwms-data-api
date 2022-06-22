package cwms.radar.data.dto;

import cwms.radar.api.errors.FieldException;

public class SpecifiedLevel implements CwmsDTO
{
	private String id;
	private String officeId;
	private String description;

	public SpecifiedLevel()
	{

	}

	public SpecifiedLevel(String id, String officeId, String description)
	{
		this.id = id;
		this.officeId = officeId;
		this.description = description;
	}

	public String getId()
	{
		return id;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getDescription()
	{
		return description;
	}

	@Override
	public boolean equals(Object o)
	{
		if(this == o)
		{
			return true;
		}
		if(o == null || getClass() != o.getClass())
		{
			return false;
		}

		final SpecifiedLevel that = (SpecifiedLevel) o;

		if(getId() != null ? !getId().equals(that.getId()) : that.getId() != null)
		{
			return false;
		}
		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		return getDescription() != null ? getDescription().equals(
				that.getDescription()) : that.getDescription() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getId() != null ? getId().hashCode() : 0;
		result = 31 * result + (getOfficeId() != null ? getOfficeId().hashCode() : 0);
		result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
		return result;
	}

	@Override
	public void validate() throws FieldException
	{

	}
}


