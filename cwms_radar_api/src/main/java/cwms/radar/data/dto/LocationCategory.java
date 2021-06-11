package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a location category")
@XmlRootElement(name="location_category")
@XmlAccessorType(XmlAccessType.FIELD)
public class LocationCategory implements CwmsDTO
{
	private final String officeId;
	private final String id;
	private final String description;

	public LocationCategory(String catDbOfficeId, String locCategoryId){
		this(catDbOfficeId, locCategoryId, null);
	}

	public LocationCategory(String catDbOfficeId, String locCategoryId, String locCategoryDesc)
	{
		this.officeId = catDbOfficeId;
		this.id = locCategoryId;
		this.description = locCategoryDesc;
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
