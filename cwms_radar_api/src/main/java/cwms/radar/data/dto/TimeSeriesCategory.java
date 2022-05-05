package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import cwms.radar.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a TimeSeries category")
@XmlRootElement(name="timeseries-category")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeSeriesCategory implements CwmsDTO
{
	private final String officeId;
	private final String id;
	private final String description;

	public TimeSeriesCategory(String catOfficeId, String catId, String catDesc)
	{
		this.officeId = catOfficeId;
		this.id = catId;
		this.description = catDesc;
	}

	public TimeSeriesCategory(TimeSeriesCategory other){
		this(other.getOfficeId(), other.getId(), other.getDescription());
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getId()
	{
		return id;
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

		final TimeSeriesCategory that = (TimeSeriesCategory) o;

		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		if(getId() != null ? !getId().equals(that.getId()) : that.getId() != null)
		{
			return false;
		}
		return getDescription() != null ? getDescription().equals(
				that.getDescription()) : that.getDescription() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getOfficeId() != null ? getOfficeId().hashCode() : 0;
		result = 31 * result + (getId() != null ? getId().hashCode() : 0);
		result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
		return result;
	}

	@Override
	public void validate() throws FieldException {
		// TODO Auto-generated method stub

	}
}
