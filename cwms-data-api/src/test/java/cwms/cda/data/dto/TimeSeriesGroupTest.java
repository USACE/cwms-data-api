package cwms.cda.data.dto;

import java.util.ArrayList;
import java.util.List;

import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import org.junit.jupiter.api.Test;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeSeriesGroupTest
{

	@Test
	void test_serialize_json(){
		TimeSeriesGroup group = buildTimeSeriesGroup();

		ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
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

    @Test
    void test_serialize_with_nulls() {
        TimeSeriesGroup group = buildTimeSeriesGroup();
        List<AssignedTimeSeries> assignedTimeSeries = new ArrayList<>();
        AssignedTimeSeries timeSeries = new AssignedTimeSeries("SPK",
            "BIG MUDDY.Elev.Total.1Day.1Day.CWMS", null, null, 0);
        assignedTimeSeries.add(timeSeries);
        TimeSeriesGroup groupWithNulls = new TimeSeriesGroup(group, assignedTimeSeries);

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
        String result = Formats.format(contentType, groupWithNulls);
        assertNotNull(result);

        assertTrue(result.contains("catOfficeId"));
        assertTrue(result.contains("catId"));
        assertTrue(result.contains("catDesc"));

        assertTrue(result.contains("grpOfficeId"));
        assertTrue(result.contains("grpId"));
        assertTrue(result.contains("grpDesc"));
        assertTrue(result.contains("grpSharedTsAliasId"));
        assertTrue(result.contains("grpSharedRefTsId"));

        assertFalse(result.contains("null"));
    }


	private TimeSeriesGroup buildTimeSeriesGroup()
	{
		TimeSeriesCategory category = new TimeSeriesCategory(
				"catOfficeId", "catId",  "catDesc"
		);

		return new TimeSeriesGroup(category,
				"grpOfficeId", "grpId", "grpDesc",
				"grpSharedTsAliasId", "grpSharedRefTsId"
				);
	}
}
