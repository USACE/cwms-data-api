package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

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
}
