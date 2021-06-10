package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a location group")
@XmlRootElement(name="location_group")
@XmlAccessorType(XmlAccessType.FIELD)
public class LocationGroup implements CwmsDTO
{
	private final String id;
	private final LocationCategory locationCategory;
	private final String officeId;
	private final String description;

	private String sharedLocAliasId;
	private String sharedRefLocationId;
	private Double locGroupAttribute;

	public LocationGroup(String catDbOfficeId, String locCategoryId, String locCategoryDesc,
		String grpDbOfficeId, String locGroupId, String locGroupDesc,
		String sharedLocAliasId, String sharedRefLocationId, Double locGroupAttribute){
			this(new LocationCategory(catDbOfficeId, locCategoryId, locCategoryDesc),
					grpDbOfficeId, locGroupId, locGroupDesc,
					sharedLocAliasId, sharedRefLocationId, locGroupAttribute);
		}

	public LocationGroup(LocationCategory cat, String grpOfficeId, String grpId, String grpDesc,
						 String sharedLocAliasId, String sharedRefLocationId, Double locGroupAttribute)
	{
		this.locationCategory = cat;
		this.officeId = grpOfficeId;
		this.id = grpId;
		this.description = grpDesc;
		this.sharedLocAliasId = sharedLocAliasId;
		this.sharedRefLocationId = sharedRefLocationId;
		this.locGroupAttribute = locGroupAttribute;
	}

	public String getId()
	{
		return id;
	}

	public LocationCategory getLocationCategory()
	{
		return locationCategory;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getDescription()
	{
		return description;
	}

	public String getSharedLocAliasId()
	{
		return sharedLocAliasId;
	}

	public String getSharedRefLocationId()
	{
		return sharedRefLocationId;
	}

	public Double getLocGroupAttribute()
	{
		return locGroupAttribute;
	}
}
