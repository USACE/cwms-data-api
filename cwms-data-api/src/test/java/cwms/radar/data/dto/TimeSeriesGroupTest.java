package cwms.radar.data.dto;

import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeSeriesGroupTest
{

	@Test
	void test_serialize_json(){
		TimeSeriesGroup group = buildTimeSeriesGroup();

		ContentType contentType = Formats.parseHeader(Formats.JSON);
		String result = Formats.format(contentType, group);
		assertNotNull(result);

		assertTrue(result.contains("catOfficeId"));
		assertTrue(result.contains("catId"));
		assertTrue(result.contains("catDesc"));

		assertTrue(result.contains("grpOfficeId"));
		assertTrue(result.contains("grpId"));
		assertTrue(result.contains("grpDesc"));
		assertTrue(result.contains("grpSharedTsAliasId"));
		assertTrue(result.contains("grpSharedRefTsId"));
	}


	private TimeSeriesGroup buildTimeSeriesGroup()
	{
		TimeSeriesCategory category = new TimeSeriesCategory(
				"catOfficeId", "catId",  "catDesc"
		);
		TimeSeriesGroup retval = new TimeSeriesGroup(category,
				"grpOfficeId", "grpId", "grpDesc",
				"grpSharedTsAliasId", "grpSharedRefTsId"
				);

		return retval;
	}
}
