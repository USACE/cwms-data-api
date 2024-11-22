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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

public class TimeSeriesProfileInstanceTest {
    private static final Map<String, String> PARAMETER_UNIT_MAP = new HashMap<>();
    @BeforeAll
    public static void setup() {
        PARAMETER_UNIT_MAP.put("Depth", "ft");
        PARAMETER_UNIT_MAP.put("Temp-Water", "F");
    }
    @Test
    void testTimeSeriesProfileInstanceSerializationRoundTrip() {
        TimeSeriesProfileInstance timeSeriesProfileInstance = buildTestTimeSeriesProfileInstance();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileInstance.class);

        String serialized = Formats.format(contentType, timeSeriesProfileInstance);
        TimeSeriesProfileInstance deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileInstance.class), serialized, TimeSeriesProfileInstance.class);
        testAssertEquals(timeSeriesProfileInstance, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testTimeSeriesProfileInstanceSerializationRoundTripFromFile() throws Exception {
        TimeSeriesProfileInstance timeSeriesProfileInstance = buildTestTimeSeriesProfileInstance();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileinstance.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileInstance deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileInstance.class), serialized, TimeSeriesProfileInstance.class);
        testAssertEquals(timeSeriesProfileInstance, deserialized, "Roundtrip serialization from file failed");
        assertEquals(3, timeSeriesProfileInstance.getTimeSeriesList().size());
        assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(Instant.ofEpochMilli(1612869582120L).toEpochMilli()).size());
        assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(Instant.ofEpochMilli(1612869682120L).toEpochMilli()).size());
        assertEquals(2, timeSeriesProfileInstance.getTimeSeriesList().get(Instant.ofEpochMilli(1612870682120L).toEpochMilli()).size());
    }

    private TimeSeriesProfileInstance buildTestTimeSeriesProfileInstance() {
        TimeSeriesProfile timeSeriesProfile = TimeSeriesProfileTest.buildTestTimeSeriesProfile();
        List<ParameterColumnInfo> parameterColumnInfoList = buildTimeSeriesList(timeSeriesProfile.getParameterList());
        List<DataColumnInfo> dataColumnInfoList = buildDataColumnInfoList();
        Map<Long, List<TimeSeriesData>> timeSeriesList = buildTimeSeriesList();
        return new TimeSeriesProfileInstance.Builder()
                .withTimeSeriesProfile(timeSeriesProfile)
                .withParameterColumns(parameterColumnInfoList)
                .withDataColumns(dataColumnInfoList)
                .withPageSize(10)
                .withTotal(100)
                .withVersion("Obs")
                .withVersionDate(Instant.parse("2020-07-15T04:06:40.00Z"))
                .withLocationTimeZone("PST")
                .withPageFirstDate(Instant.parse("2020-07-09T12:00:00.00Z"))
                .withPageLastDate(Instant.parse("2025-07-09T12:00:00.00Z"))
                .withTimeSeriesList(timeSeriesList)
                .withFirstDate(Instant.parse("2020-07-09T12:00:00.00Z"))
                .withLastDate(Instant.parse("2025-07-09T12:00:00.00Z"))
                .build();
    }

    private List<DataColumnInfo> buildDataColumnInfoList() {
        List<DataColumnInfo> dataColumnInfoList = new ArrayList<>();
        dataColumnInfoList.add(new DataColumnInfo.Builder().withName("value").withOrdinal(1).withDataType("java.lang.Double").build());
        dataColumnInfoList.add(new DataColumnInfo.Builder().withName("quality").withOrdinal(2).withDataType("java.lang.Integer").build());
        return dataColumnInfoList;
    }

    private Map<Long, List<TimeSeriesData>> buildTimeSeriesList() {
        Map<Long, List<TimeSeriesData>> timeSeriesList = new TreeMap<>();
        timeSeriesList.put(Instant.parse("2021-02-09T11:19:42.12Z").toEpochMilli(), buildTimeValueList(1));
        timeSeriesList.put(Instant.parse("2021-02-09T11:21:22.12Z").toEpochMilli(), buildTimeValueList(2));
        timeSeriesList.put(Instant.parse("2021-02-09T11:38:02.12Z").toEpochMilli(), buildTimeValueList(3));
        return timeSeriesList;
    }

    private List<ParameterColumnInfo> buildTimeSeriesList(List<String> parameterList) {
        List<ParameterColumnInfo> parameterColumnInfoList = new ArrayList<>();
        int count = 0;
        for(String parameter : parameterList)
        {
            ParameterColumnInfo columnInfo = new ParameterColumnInfo.Builder()
                    .withParameter(parameter)
                    .withOrdinal(count)
                    .withUnit(PARAMETER_UNIT_MAP.get(parameter)).build();
            parameterColumnInfoList.add(columnInfo);
            count++;
        }
        return parameterColumnInfoList;
    }

    private List<TimeSeriesData> buildTimeValueList(int i) {
        List<TimeSeriesData> timeValueList = new ArrayList<>();
        switch(i){
            case 1:
                timeValueList.add(new TimeSeriesData(86.5, 0));
                timeValueList.add(new TimeSeriesData(98.6, 0));
                break;
            case 2:
                timeValueList.add(new TimeSeriesData(86.999, 0));
                timeValueList.add(new TimeSeriesData(98.6, 0));
                break;
            default:
                timeValueList.add(null);
                timeValueList.add(new TimeSeriesData(98.6, 0));
                break;
        }
        return timeValueList;
    }

    private void testAssertEquals(TimeSeriesProfileInstance expected, TimeSeriesProfileInstance actual, String message) {
        TimeSeriesProfileTest.testAssertEquals(expected.getTimeSeriesProfile(), actual.getTimeSeriesProfile(), message);
        testAssertEquals(expected.getTimeSeriesList(), actual.getTimeSeriesList(), message);
        assertEquals(expected.getFirstDate(), actual.getFirstDate());
        assertEquals(expected.getLastDate(), actual.getLastDate());
    }

    private void testAssertEquals(Map<Long, List<TimeSeriesData>> expected, Map<Long, List<TimeSeriesData>> actual, String message) {
        assertEquals(expected.size(), actual.size(), message);
        Iterator<Long> expectedIterator = expected.keySet().iterator();
        Iterator<Long> actualIterator = actual.keySet().iterator();
        while(expectedIterator.hasNext())
        {
            Long expectedKey = expectedIterator.next();
            Long actualKey = actualIterator.next();
            assertEquals(expectedKey, actualKey);
            testAssertEquals(expected.get(expectedKey), actual.get(actualKey), message);
        }
    }

    private void testAssertEquals(List<TimeSeriesData> expected, List<TimeSeriesData> actual, String message) {
        assertEquals(expected.size(), actual.size(), message);
        for (TimeSeriesData data : expected) {
            boolean found = false;
            for (TimeSeriesData actualData : actual) {
                if (data == null && actualData == null) {
                    found = true;
                    break;
                }
                if (data != null && actualData != null && data.getValue() == actualData.getValue() && data.getQuality() == actualData.getQuality()) {
                        found = true;
                        break;
                    }
            }
            if (!found) {
                fail("Expected timeseries value data not found in actual data");
            }
        }
    }
}
