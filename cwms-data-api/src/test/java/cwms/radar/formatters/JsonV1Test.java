package cwms.radar.formatters;

import java.util.ArrayList;
import java.util.List;

import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.json.JsonV1;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JsonV1Test
{

	@Test
	void singleOfficeFormat()
	{
		Office office = new Office("name_here", "long_name_next", "office_type_third", "reports_to_last");
		JsonV1 v1 = new JsonV1();
		String result = v1.format(office);
		assertNotNull(result);
		assertTrue(result.contains("offices"));
	}

	@Test
	void listOfficeFormat()
	{
		List<Office> offices = buildOffices();

		JsonV1 v1 = new JsonV1();
		String result = v1.format(offices);
		assertNotNull(result);
		assertTrue(result.contains("offices"));
	}


	private List<Office> buildOffices()
	{
		String name = "name";
		String longName = "long_name";
		String officeType = "office_type";
		String reportsTo = "reports_to";

		List<Office> offices = new ArrayList<>();
		for(int i = 0; i < 4; i++)
		{
			Office office = new Office(name + i, longName + i, officeType + i, reportsTo + i);
			offices.add(office);
		}
		return offices;
	}


	@Test
	void singleLocCatFormat()
	{
		LocationCategory cat = new LocationCategory("catOfficeId", "id", "catDesc");
		JsonV1 v1 = new JsonV1();
		String result = v1.format(cat);
		assertNotNull(result);
		assertTrue(result.contains("catOfficeId"));
	}

	@Test
	void listCatsFormat()
	{
		List<LocationCategory> cats = buildCats();

		JsonV1 v1 = new JsonV1();
		String result = v1.format(cats);
		assertNotNull(result);
		assertTrue(result.contains("catOfficeId"));
	}


	private List<LocationCategory> buildCats()
	{
		List<LocationCategory> cats = new ArrayList<>();
		for(int i = 0; i < 4; i++)
		{
			LocationCategory cat = new LocationCategory("catOfficeId" + i, "id" + i, "catDesc");
			cats.add(cat);
		}
		return cats;
	}


	@Test
	void singleLocGroFormat()
	{
		LocationCategory cat = new LocationCategory("catOfficeId", "id", "catDesc");
		final Double locGroupAttribute = null;
		LocationGroup grp = new LocationGroup(cat, "grpOfficeId", "id", "grpDesc", "sharedLocAliasId",
				"sharedRefLocationId", locGroupAttribute);
		JsonV1 v1 = new JsonV1();
		String result = v1.format(cat);
		assertNotNull(result);
		assertTrue(result.contains("catOfficeId"));
	}

	@Test
	void listGrpsFormat()
	{
		List<LocationGroup> cats = buildGrps();

		JsonV1 v1 = new JsonV1();
		String result = v1.format(cats);
		assertNotNull(result);
		assertTrue(result.contains("catOfficeId"));
	}


	private List<LocationGroup> buildGrps()
	{
		List<LocationGroup> cats = new ArrayList<>();
		for(int i = 0; i < 4; i++)
		{
			LocationCategory cat = new LocationCategory("catOfficeId", "id", "catDesc");
			LocationGroup grp = new LocationGroup(cat, "grpOfficeId" + i, "id" + i, "grpDesc", null,
					null, null);
			cats.add(grp);
		}
		return cats;
	}


}
