package cwms.cda.data.dto.timeseriesprofile;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimeSeriesProfileInstanceTest {
    private static final Map<String, String> PARAMETER_UNIT_MAP = new HashMap<>();
    @BeforeAll
    public static void setup() throws Exception {
        PARAMETER_UNIT_MAP.put("Depth", "ft");
        PARAMETER_UNIT_MAP.put("Temperature", "F");
    }
    @Test
    void testTimeSeriesProfileInstanceSerializationRoundTrip() {
        TimeSeriesProfileInstance timeSeriesProfileInstance = buildTestTimeSeriesProfileInstance();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileInstance.class);

        String serialized = Formats.format(contentType, timeSeriesProfileInstance);
        TimeSeriesProfileInstance deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileInstance.class), serialized, TimeSeriesProfileInstance.class);
        testAssertEquals(timeSeriesProfileInstance, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testTimeSeriesProfileInstanceSerializationRoundTripFromFile() throws Exception {
        TimeSeriesProfileInstance timeSeriesProfileInstance = buildTestTimeSeriesProfileInstance();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileinstance.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileInstance deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileInstance.class), serialized, TimeSeriesProfileInstance.class);
        testAssertEquals(timeSeriesProfileInstance, deserialized, "Roundtrip serialization from file failed");
    }

    private TimeSeriesProfileInstance buildTestTimeSeriesProfileInstance() {
        TimeSeriesProfile timeSeriesProfile = TimeSeriesProfileTest.buildTestTimeSeriesProfile();
        List<ProfileTimeSeries> timeSeriesList = buildTimeSeriesList(timeSeriesProfile.getParameterList());
        return new TimeSeriesProfileInstance.Builder()
                .withTimeSeriesProfile(timeSeriesProfile)
                .withTimeSeriesList(timeSeriesList)
                .build();
    }

    private List<ProfileTimeSeries> buildTimeSeriesList(List<String> parameterList) {
        List<ProfileTimeSeries> timeSeriesList = new ArrayList<>();
        for(String parameter : parameterList)
        {
            ProfileTimeSeries profileTimeSeries = new ProfileTimeSeries.Builder()
                    .withParameter(parameter)
                    .withUnit(PARAMETER_UNIT_MAP.get(parameter))
                    .withTimeZone("PST")
                    .withTimeValuePairList(buildTimeValueList())
                    .build();
            timeSeriesList.add(profileTimeSeries);
        }
        return timeSeriesList;
    }

    private List<TimeValuePair> buildTimeValueList() {
        List<TimeValuePair> timeValueList = new ArrayList<>();
        timeValueList.add( new TimeValuePair.Builder().withValue(1.0).withDateTime(Instant.parse("2021-02-09T11:19:42.12Z")).build());
        timeValueList.add( new TimeValuePair.Builder().withValue(3.0).withDateTime(Instant.parse("2021-02-09T11:19:42.22Z")).build());
        return timeValueList;
    }

    private void testAssertEquals(TimeSeriesProfileInstance expected, TimeSeriesProfileInstance actual, String message) {
        TimeSeriesProfileTest.testAssertEquals(expected.getTimeSeriesProfile(), actual.getTimeSeriesProfile(), message);
        testAssertEquals(expected.getTimeSeriesList(), actual.getTimeSeriesList(), message);
    }

    private void testAssertEquals(List<ProfileTimeSeries> expected, List<ProfileTimeSeries> actual, String message) {
        assertEquals(expected.size(), actual.size(), message);
        for (int i = 0; i < expected.size(); i++) {
            testAssertEquals(expected.get(i), actual.get(i), message);
        }
    }

    private void testAssertEquals(ProfileTimeSeries expected, ProfileTimeSeries actual, String message) {
        assertEquals(expected.getTimeZone(), actual.getTimeZone(), message);
        assertEquals(expected.getParameter(), actual.getParameter(), message);
        assertEquals(expected.getUnit(),actual.getUnit(), message);
        testAssertEquals(expected.getTimeValuePairList(), actual.getTimeValuePairList());
    }

    private void testAssertEquals(List<TimeValuePair> expected, List<TimeValuePair> actual) {
        assertEquals(expected.size(), actual.size());
        for(int i=0;i<expected.size();i++)
        {
            assertEquals(expected.get(i).getDateTime(), actual.get(i).getDateTime());
            assertEquals(expected.get(i).getValue(), actual.get(i).getValue());
        }
    }
}
