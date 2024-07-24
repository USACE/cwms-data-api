package cwms.cda.data.dto.timeseriesprofile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class TimeSeriesProfileParserTest {
    @Test
    void testTimeSeriesProfileSerializationRoundTrip() {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class);

        String serialized = Formats.format(contentType, timeSeriesProfileParser);
        TimeSeriesProfileParser deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class), serialized, TimeSeriesProfileParser.class);
        testAssertEquals(timeSeriesProfileParser, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testTimeSeriesProfileColumnarSerializationRoundTrip() {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParserColumnar();
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class);

        String serialized = Formats.format(contentType, timeSeriesProfileParser);
        TimeSeriesProfileParser deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class), serialized, TimeSeriesProfileParser.class);
        testAssertEquals(timeSeriesProfileParser, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testTimeSeriesProfileSerializationRoundTripFromFile() throws Exception {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileparser.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileParser deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class), serialized, TimeSeriesProfileParser.class);
        testAssertEquals(timeSeriesProfileParser, deserialized, "Roundtrip serialization from file failed");
    }
    @Test
    void testTimeSeriesProfileSerializationRoundTripColumnarFromFile() throws Exception {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParserColumnar();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileparsercolumnar.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileParser deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, TimeSeriesProfileParser.class), serialized, TimeSeriesProfileParser.class);
        testAssertEquals(timeSeriesProfileParser, deserialized, "Roundtrip serialization from file failed");
    }

    private static TimeSeriesProfileParser buildTestTimeSeriesProfileParser() {
        List<ParameterInfo> parameterInfo = new ArrayList<>();
        parameterInfo.add(new ParameterInfo.Builder()
                .withParameter("Depth")
                .withIndex(3)
                .withUnit("m")
                .build());
        parameterInfo.add(new ParameterInfo.Builder()
                .withParameter("Temp-Water")
                .withIndex(5)
                .withUnit("F")
                .build());
        CwmsId locationId = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("location")
                .build();
        return
                new TimeSeriesProfileParser.Builder()
                        .withLocationId(locationId)
                        .withKeyParameter("Depth")
                        .withRecordDelimiter((char) 10)
                        .withFieldDelimiter(',')
                        .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                        .withTimeField(1)
                        .withTimeInTwoFields(false)
                        .withParameterInfoList(parameterInfo)
                        .build();
    }
    private static TimeSeriesProfileParser buildTestTimeSeriesProfileParserColumnar() {
        List<ParameterInfo> parameterInfo = new ArrayList<>();
        parameterInfo.add(new ParameterInfo.Builder()
                .withParameter("Depth")
                .withUnit("m")
                .withStartColumn(11)
                .withEndColumn(20)
                .build());
        parameterInfo.add(new ParameterInfo.Builder()
                .withParameter("Temp-Water")
                .withUnit("F")
                .withStartColumn(21)
                .withEndColumn(30)
                .build());
        CwmsId locationId = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("location")
                .build();
        return
                new TimeSeriesProfileParser.Builder()
                        .withLocationId(locationId)
                        .withKeyParameter("Depth")
                        .withRecordDelimiter((char) 10)
                        .withTimeStartColumn(1)
                        .withTimeEndColumn(10)
                        .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                        .withTimeInTwoFields(false)
                        .withParameterInfoList(parameterInfo)
                        .build();
    }

    private void testAssertEquals(TimeSeriesProfileParser expected, TimeSeriesProfileParser actual, String message) {
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getFieldDelimiter(), actual.getFieldDelimiter(), message);
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getKeyParameter(), actual.getKeyParameter(), message);
        assertEquals(expected.getTimeField(), actual.getTimeField(), message);
        assertEquals(expected.getTimeFormat(), actual.getTimeFormat(), message);
        testAssertEquals(expected.getParameterInfoList(), actual.getParameterInfoList(),message);
        assertEquals(expected.getRecordDelimiter(), actual.getRecordDelimiter(), message);
        assertEquals(expected.getTimeZone(), actual.getTimeZone());
        assertEquals(expected.getTimeInTwoFields(), actual.getTimeInTwoFields());
        assertEquals(expected.getTimeEndColumn(), actual.getTimeEndColumn());
        assertEquals(expected.getTimeStartColumn(), actual.getTimeStartColumn());
    }

    private void testAssertEquals(List<ParameterInfo> expected, List<ParameterInfo> actual, String message) {
        assertEquals(expected.size(), actual.size());
        for(int i=0;i<expected.size();i++)
        {
            assertEquals(expected.get(i).getIndex(), actual.get(i).getIndex(), message);
            assertEquals(expected.get(i).getParameter(), actual.get(i).getParameter(), message);
            assertEquals(expected.get(i).getUnit(), actual.get(i).getUnit(), message);
            assertEquals(expected.get(i).getStartColumn(), actual.get(i).getStartColumn());
            assertEquals(expected.get(i).getEndColumn(), actual.get(i).getEndColumn());
        }
    }
}
