package cwms.cda.formatters;

import java.util.List;

import cwms.cda.data.dto.LocationCategory;

public class LocationCategoryFormatV1
{
	private final List<LocationCategory> locationCategories;

	public LocationCategoryFormatV1(List<LocationCategory> cats)
	{
		this.locationCategories = cats;
	}

	public List<LocationCategory> getLocationCategories()
	{
		return locationCategories;
	}
}
