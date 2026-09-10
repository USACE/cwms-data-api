package cwms.cda.data.dto;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeSeriesGroupTest
{
    @Test
    void displayUnitsRoundTrip() {
        AssignedTimeSeries assigned = new AssignedTimeSeries("SPK", "Cedar.Solar.Inst.1Hour.0.Test",
            null, null, 1, "W/m2", "EN");
        TimeSeriesGroup group = new TimeSeriesGroup(buildTimeSeriesGroup(), List.of(assigned));
        ContentType type = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
        String json = Formats.format(type, group);
        assertTrue(json.contains("\"units\":\"W/m2\""));
        assertTrue(json.contains("\"unit-system\":\"EN\""));
        TimeSeriesGroup parsed = Formats.parseContent(type, json, TimeSeriesGroup.class);
        assertEquals("W/m2", parsed.getAssignedTimeSeries().get(0).getUnits());
        assertEquals("EN", parsed.getAssignedTimeSeries().get(0).getUnitSystem());
    }

    @Test
    void omittedUnitsPreservePreferenceAndExplicitNullClearsIt() {
        ContentType type = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
        TimeSeriesGroup group = new TimeSeriesGroup(buildTimeSeriesGroup(), List.of(new AssignedTimeSeries(
            "SPK", "Cedar.Stage.Inst.1Hour.0.Test", null, null, 1)));
        String omitted = Formats.format(type, group);
        TimeSeriesGroup preserve = Formats.parseContent(type, omitted, TimeSeriesGroup.class);
        assertFalse(preserve.getAssignedTimeSeries().get(0).isUnitsSpecified());
        String explicitNull = omitted.replace("\"timeseries-id\"", "\"units\":null,\"timeseries-id\"");
        TimeSeriesGroup clear = Formats.parseContent(type, explicitNull, TimeSeriesGroup.class);
        assertTrue(clear.getAssignedTimeSeries().get(0).isUnitsSpecified());
    }

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
        assertFalse(result.contains("\"units\""));
        assertFalse(result.contains("unit-system"));
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
