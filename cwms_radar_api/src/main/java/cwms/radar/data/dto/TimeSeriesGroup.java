package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a timeseries group")
@XmlRootElement(name="timeseries-group")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeSeriesGroup implements CwmsDTO
{
	private final String id;
	private final TimeSeriesCategory timeSeriesCategory;
	private final String officeId;
	private final String description;

	private String sharedAliasId;
	private String sharedRefTsId;



	public TimeSeriesGroup(String catDbOfficeId, String tsCategoryId, String tsCategoryDesc,
						   String grpDbOfficeId, String tsGroupId, String tsGroupDesc,
						   String sharedTsAliasId, String sharedRefTsId){
			this(new TimeSeriesCategory(catDbOfficeId, tsCategoryId, tsCategoryDesc),
					grpDbOfficeId, tsGroupId, tsGroupDesc,
					sharedTsAliasId, sharedRefTsId);
		}

	public TimeSeriesGroup(TimeSeriesCategory cat, String grpOfficeId, String grpId, String grpDesc,
						   String sharedTsAliasId, String sharedRefTsId)
	{
		this.timeSeriesCategory = cat;
		this.officeId = grpOfficeId;
		this.id = grpId;
		this.description = grpDesc;
		this.sharedAliasId = sharedTsAliasId;
		this.sharedRefTsId = sharedRefTsId;

	}

	public String getId()
	{
		return id;
	}

	public TimeSeriesCategory getTimeSeriesCategory()
	{
		return timeSeriesCategory;
	}

	public String getDescription()
	{
		return description;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getSharedAliasId()
	{
		return sharedAliasId;
	}

	public String getSharedRefTsId()
	{
		return sharedRefTsId;
	}


}
