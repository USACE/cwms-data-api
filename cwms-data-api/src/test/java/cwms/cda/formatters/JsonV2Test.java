package cwms.cda.formatters;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.formatters.json.JsonV2;

class JsonV2Test extends TimeSeriesTestBase {

    @Override
    public OutputFormatter getOutputFormatter() {
        return new JsonV2();
    }

    @Test
    @Override
    public void singleTimeseriesFormat() {
        super.singleTimeseriesFormat();
    }

    @Test
    void canSerializeLocationLevel(){
        String crazyName = "crazyName" + System.nanoTime();
        LocationLevel level = buildLevel(crazyName);

        String formatted = new JsonV2().format(level);
        assertNotNull(formatted);
        assertTrue(formatted.contains(crazyName));

        // Make sure that Formats can format it when asked to use JSONV2
        // This will fail if JSONv2 is missing the class in the @FormatService annotation.
        ContentType contentType = new ContentType(Formats.JSONV2);
        String formatted2 = Formats.format(contentType, level);
        assertNotNull(formatted2);
        assertTrue(formatted2.contains(crazyName));
    }

    private LocationLevel buildLevel(String crazyName) {
        ZonedDateTime efDate = ZonedDateTime.parse("2021-06-21T08:00:00-07:00[PST8PDT]");
        return new LocationLevel.Builder(crazyName, efDate).build();
    }

    @Test
    void canSerializeLocationLevels(){

        String crazyName = "crazyName" + System.nanoTime();

        LocationLevels levels = buildLevels(crazyName);

        // make sure that JsonV2 can format it.
        String formatted = new JsonV2().format(levels);
        assertNotNull(formatted);
        assertTrue(formatted.contains(crazyName));

        // Make sure that Formats can format it when asked to use JSONV2
        // This will fail if JSONv2 is missing the class in the @FormatService annotation.
        ContentType contentType = new ContentType(Formats.JSONV2);
        String formatted2 = Formats.format(contentType, levels);
        assertNotNull(formatted2);
        assertTrue(formatted2.contains(crazyName));

    }

    private LocationLevels buildLevels(String crazyName) {
        LocationLevel level = buildLevel(crazyName);

        int offset = 0;
        int pageSize = 500;
        Integer total = null;
        return new LocationLevels.Builder(offset, pageSize, total).add(level).build();
    }

}
