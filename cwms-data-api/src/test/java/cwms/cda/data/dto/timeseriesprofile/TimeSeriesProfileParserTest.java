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
    void testTimeSeriesProfileColumnarSerializationRoundTrip() {
        TimeSeriesProfileParserColumnar timeSeriesProfileParser = buildTestTimeSeriesProfileParserColumnar();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserColumnar.class);

        String serialized = Formats.format(contentType, timeSeriesProfileParser);
        TimeSeriesProfileParserColumnar deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserColumnar.class), serialized, TimeSeriesProfileParserColumnar.class);
        testAssertEquals(timeSeriesProfileParser,  deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testTimeSeriesProfileIndexedSerializationRoundTrip() {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed();
        ContentType contentType = Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserColumnar.class);

        String serialized = Formats.format(contentType, timeSeriesProfileParser);
        TimeSeriesProfileParserIndexed deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserIndexed.class), serialized, TimeSeriesProfileParserIndexed.class);
        testAssertEquals((TimeSeriesProfileParserIndexed)timeSeriesProfileParser, deserialized, "Roundtrip serialization failed");
    }
    @Test
    void testTimeSeriesProfileSerializationRoundTripColumnarFromFile() throws Exception {
        TimeSeriesProfileParserColumnar timeSeriesProfileParser = buildTestTimeSeriesProfileParserColumnar();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileparsercolumnar.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileParserColumnar deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserColumnar.class), serialized, TimeSeriesProfileParserColumnar.class);
        testAssertEquals(timeSeriesProfileParser,  deserialized, "Roundtrip serialization from file failed");
    }

    @Test
    void testTimeSeriesProfileSerializationRoundTripIndexedFromFile() throws Exception {
        TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParserIndexed();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofileparserindexed.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        TimeSeriesProfileParserIndexed deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesProfileParserIndexed.class), serialized, TimeSeriesProfileParserIndexed.class);
        testAssertEquals((TimeSeriesProfileParserIndexed) timeSeriesProfileParser, deserialized, "Roundtrip serialization from file failed");
    }

    private static TimeSeriesProfileParserColumnar buildTestTimeSeriesProfileParserColumnar() {
        List<ParameterInfo> parameterInfo = new ArrayList<>();
        parameterInfo.add(new ParameterInfoColumnar.Builder()
                .withStartColumn(11)
                .withEndColumn(20)
                .withParameter("Depth")
                .withUnit("m")
               .build());
        parameterInfo.add(new ParameterInfoColumnar.Builder()
                .withStartColumn(21)
                .withEndColumn(30)
                .withParameter("Temp-Water")
                .withUnit("F")
                 .build());
        CwmsId locationId = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("location")
                .build();
        return (TimeSeriesProfileParserColumnar)
                new TimeSeriesProfileParserColumnar.Builder()
                        .withTimeStartColumn(1)
                        .withTimeEndColumn(10)
                        .withLocationId(locationId)
                        .withKeyParameter("Depth")
                        .withRecordDelimiter((char) 10)
                        .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                        .withTimeInTwoFields(false)
                        .withParameterInfoList(parameterInfo)
                        .build();
    }
    private static TimeSeriesProfileParser buildTestTimeSeriesProfileParserIndexed() {
        List<ParameterInfo> parameterInfo = new ArrayList<>();
        parameterInfo.add(new ParameterInfoIndexed.Builder()
                .withIndex(3)
                .withParameter("Depth")
                .withUnit("m")
                .build());
        parameterInfo.add(new ParameterInfoIndexed.Builder()
                .withIndex(5)
                .withParameter("Temp-Water")
                .withUnit("F")
                .build());
        CwmsId locationId = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("location")
                .build();
        return
                new TimeSeriesProfileParserIndexed.Builder()
                        .withTimeField(1L)
                        .withFieldDelimiter(',')
                        .withLocationId(locationId)
                        .withKeyParameter("Depth")
                        .withRecordDelimiter((char) 10)
                        .withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
                        .withTimeZone("UTC")
                        .withTimeInTwoFields(false)
                        .withParameterInfoList(parameterInfo)
                        .build();
    }

    private void testAssertEquals(TimeSeriesProfileParserColumnar expected, TimeSeriesProfileParserColumnar actual, String message) {
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getKeyParameter(), actual.getKeyParameter(), message);
        assertEquals(expected.getTimeFormat(), actual.getTimeFormat(), message);
        testAssertEquals(expected.getParameterInfoList(), actual.getParameterInfoList(),message);
        assertEquals(expected.getRecordDelimiter(), actual.getRecordDelimiter(), message);
        assertEquals(expected.getTimeZone(), actual.getTimeZone());
        assertEquals(expected.getTimeInTwoFields(), actual.getTimeInTwoFields());
        assertEquals(expected.getTimeEndColumn(), actual.getTimeEndColumn());
        assertEquals(expected.getTimeStartColumn(), actual.getTimeStartColumn());
    }
    private void testAssertEquals(TimeSeriesProfileParserIndexed expected, TimeSeriesProfileParserIndexed actual, String message) {
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getLocationId().getName(), actual.getLocationId().getName(), message);
        assertEquals(expected.getLocationId().getOfficeId(), actual.getLocationId().getOfficeId(), message);
        assertEquals(expected.getKeyParameter(), actual.getKeyParameter(), message);
        assertEquals(expected.getTimeFormat(), actual.getTimeFormat(), message);
        testAssertEquals(expected.getParameterInfoList(), actual.getParameterInfoList(),message);
        assertEquals(expected.getRecordDelimiter(), actual.getRecordDelimiter(), message);
        assertEquals(expected.getTimeZone(), actual.getTimeZone());
        assertEquals(expected.getTimeInTwoFields(), actual.getTimeInTwoFields());
        assertEquals(expected.getTimeField(), actual.getTimeField());
        assertEquals(expected.getFieldDelimiter(), actual.getFieldDelimiter());
    }
    private void testAssertEquals(List<ParameterInfo> expected, List<ParameterInfo> actual, String message) {
        assertEquals(expected.size(), actual.size());
        for(int i=0;i<expected.size();i++)
        {
            assertEquals(expected.get(i).getParameter(), actual.get(i).getParameter(), message);
            assertEquals(expected.get(i).getUnit(), actual.get(i).getUnit(), message);
            if(expected.get(i) instanceof ParameterInfoIndexed &&  actual.get(i) instanceof ParameterInfoIndexed) {
                assertEquals(((ParameterInfoIndexed) expected.get(i)).getIndex(), ((ParameterInfoIndexed) actual.get(i)).getIndex(), message);
            }
            else if(expected.get(i) instanceof ParameterInfoColumnar &&  actual.get(i) instanceof ParameterInfoColumnar) {
                assertEquals(((ParameterInfoColumnar) expected.get(i)).getStartColumn(), ((ParameterInfoColumnar) actual.get(i)).getStartColumn(), message);
                assertEquals(((ParameterInfoColumnar) expected.get(i)).getEndColumn(), ((ParameterInfoColumnar) actual.get(i)).getEndColumn(), message);
            }
            else
            {
                assertEquals(expected.get(i).getClass(), actual.get(i).getClass());
            }
        }
    }
}
