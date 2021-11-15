package cwms.radar.data.dto;

import java.util.ArrayList;
import java.util.List;
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
	private Number locGroupAttribute;

	private List<AssignedLocation> assignedLocations = null;

	public LocationGroup(String catDbOfficeId, String locCategoryId, String locCategoryDesc,
		String grpDbOfficeId, String locGroupId, String locGroupDesc,
		String sharedLocAliasId, String sharedRefLocationId, Number locGroupAttribute){
			this(new LocationCategory(catDbOfficeId, locCategoryId, locCategoryDesc),
					grpDbOfficeId, locGroupId, locGroupDesc,
					sharedLocAliasId, sharedRefLocationId, locGroupAttribute);
	}

	public LocationGroup(LocationCategory cat, String grpOfficeId, String grpId, String grpDesc,
						 String sharedLocAliasId, String sharedRefLocationId, Number locGroupAttribute)
	{
		this.locationCategory = cat;
		this.officeId = grpOfficeId;
		this.id = grpId;
		this.description = grpDesc;
		this.sharedLocAliasId = sharedLocAliasId;
		this.sharedRefLocationId = sharedRefLocationId;
		this.locGroupAttribute = locGroupAttribute;
	}

	public LocationGroup(LocationGroup group, List<AssignedLocation> locs){
		this(group);
		if(locs != null && !locs.isEmpty())
		{
			this.assignedLocations = new ArrayList<>(locs);
		}
	}

	public LocationGroup(LocationGroup group){
		this.locationCategory = group.getLocationCategory();
		this.officeId = group.getOfficeId();
		this.id = group.getId();
		this.description = group.getDescription();
		this.sharedLocAliasId = group.getSharedLocAliasId();
		this.sharedRefLocationId = group.getSharedRefLocationId();
		this.locGroupAttribute = group.getLocGroupAttribute();
		List<AssignedLocation> locs = group.getAssignedLocations();
		if(locs != null && !locs.isEmpty())
		{
			this.assignedLocations = new ArrayList<>(locs);
		}
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

	public Number getLocGroupAttribute()
	{
		return locGroupAttribute;
	}

	public List<AssignedLocation> getAssignedLocations()
	{
		return assignedLocations;
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

		final LocationGroup that = (LocationGroup) o;

		if(getId() != null ? !getId().equals(that.getId()) : that.getId() != null)
		{
			return false;
		}
		if(getLocationCategory() != null ? !getLocationCategory().equals(
				that.getLocationCategory()) : that.getLocationCategory() != null)
		{
			return false;
		}
		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		if(getSharedLocAliasId() != null ? !getSharedLocAliasId().equals(
				that.getSharedLocAliasId()) : that.getSharedLocAliasId() != null)
		{
			return false;
		}
		if(getSharedRefLocationId() != null ? !getSharedRefLocationId().equals(
				that.getSharedRefLocationId()) : that.getSharedRefLocationId() != null)
		{
			return false;
		}
		if(getLocGroupAttribute() != null ? !getLocGroupAttribute().equals(
				that.getLocGroupAttribute()) : that.getLocGroupAttribute() != null)
		{
			return false;
		}
		return getAssignedLocations() != null ? getAssignedLocations().equals(
				that.getAssignedLocations()) : that.getAssignedLocations() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getId() != null ? getId().hashCode() : 0;
		result = 31 * result + (getLocationCategory() != null ? getLocationCategory().hashCode() : 0);
		result = 31 * result + (getOfficeId() != null ? getOfficeId().hashCode() : 0);
		result = 31 * result + (getSharedLocAliasId() != null ? getSharedLocAliasId().hashCode() : 0);
		result = 31 * result + (getSharedRefLocationId() != null ? getSharedRefLocationId().hashCode() : 0);
		result = 31 * result + (getLocGroupAttribute() != null ? getLocGroupAttribute().hashCode() : 0);
		result = 31 * result + (getAssignedLocations() != null ? getAssignedLocations().hashCode() : 0);
		return result;
	}
}
