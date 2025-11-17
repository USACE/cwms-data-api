package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimeSeriesVerticalDatumConverterTest {

    @Test
    void testConvertVerticalDatum() throws Exception
    {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/ts_with_vertical.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        TimeSeries ts = Formats.parseContent(contentType, tsData, TimeSeries.class);

        Double conversionFactor = Arrays.stream(ts.getVerticalDatumInfo().getOffsets()).sequential()
                .filter(o -> o.getToDatum().equalsIgnoreCase("NAVD-88"))
                .findFirst()
                .map(VerticalDatumInfo.Offset::getValue)
                .orElseThrow(() -> new Exception("No conversion factor from NGVD29 to NAVD88 found"));
        TimeSeries convertedTs = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(ts, VerticalDatum.NAVD88);
        assertFalse(convertedTs.getValues().isEmpty());
        assertEquals(convertedTs.getValues().size(), ts.getValues().size());
        for(int i=0; i< convertedTs.getValues().size(); i++)
        {
            assertEquals(ts.getValues().get(i).getValue() + conversionFactor, convertedTs.getValues().get(i).getValue(), 0.0001);
        }
        assertEquals(ts.getVerticalDatumInfo().getElevation() + conversionFactor, convertedTs.getVerticalDatumInfo().getElevation(), 0.0001);
        assertEquals("NAVD-88", convertedTs.getVerticalDatumInfo().getNativeDatum());
    }

    @Test
    void testConvertVerticalDatumRoundTrip() throws Exception
    {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/ts_with_vertical.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        TimeSeries ts = Formats.parseContent(contentType, tsData, TimeSeries.class);
        TimeSeries convertedTs = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(ts, VerticalDatum.NAVD88);
        TimeSeries convertedTsBack = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(convertedTs, VerticalDatum.NGVD29);
        
        //verify round trip worked
        assertFalse(convertedTsBack.getValues().isEmpty());
        assertEquals(convertedTsBack.getValues().size(), ts.getValues().size());
        for(int i=0; i< convertedTsBack.getValues().size(); i++)
        {
            assertEquals(convertedTsBack.getValues().get(i).getValue(), ts.getValues().get(i).getValue(), 0.0001);
        }
        assertEquals(ts.getVerticalDatumInfo().getElevation(), convertedTsBack.getVerticalDatumInfo().getElevation(), 0.0001);
        assertEquals(ts.getVerticalDatumInfo().getNativeDatum(), convertedTsBack.getVerticalDatumInfo().getNativeDatum());
    }

    @Test
    void testConvertVerticalDatumOffsetsUpdates() throws Exception
    {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/ts_with_vertical.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeries.class);
        TimeSeries ts = Formats.parseContent(contentType, tsData, TimeSeries.class);
        VerticalDatumInfo originalDatumInfo = ts.getVerticalDatumInfo();
        List<VerticalDatumInfo.Offset> madeupOffsets = new ArrayList<>(Arrays.asList(originalDatumInfo.getOffsets()));
        //since we are tied to an enum to define what we support, using "NATIVE" as a made-up datum value for testing
        madeupOffsets.add(new VerticalDatumInfo.Offset(true, VerticalDatum.NATIVE.toString(), 10.0));
        VerticalDatumInfo madeupVdi = new VerticalDatumInfo.Builder()
                .from(ts.getVerticalDatumInfo())
                .withOffsets(madeupOffsets.toArray(new VerticalDatumInfo.Offset[]{}))
                .build();
        ts = new TimeSeries(ts.getPage(),
                ts.getPageSize(),
                ts.getTotal(),
                ts.getName(),
                ts.getOfficeId(),
                ts.getBegin(),
                ts.getEnd(),
                ts.getUnits(),
                ts.getInterval(),
                madeupVdi,
                ts.getIntervalOffset(),
                ts.getTimeZone(),
                ts.getVersionDate(),
                ts.getDateVersionType())
                .withValues(ts.getValues());
        TimeSeries convertedTs = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(ts, VerticalDatum.NAVD88);
        TimeSeries convertedTsToMadeUp = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(convertedTs, VerticalDatum.NATIVE);
        TimeSeries convertBackToOriginal = TimeSeriesVerticalDatumConverter.convertToVerticalDatum(convertedTsToMadeUp, VerticalDatum.NGVD29);
        //verify we get back to original after multiple conversions between datums - this ensures that offsets are being updated properly
        assertFalse(convertBackToOriginal.getValues().isEmpty());
        assertEquals(convertBackToOriginal.getValues().size(), ts.getValues().size());
        for(int i=0; i< convertBackToOriginal.getValues().size(); i++)
        {
            assertEquals(ts.getValues().get(i).getValue(), convertBackToOriginal.getValues().get(i).getValue(), 0.0001);
        }
        assertEquals(ts.getVerticalDatumInfo().getElevation(), convertBackToOriginal.getVerticalDatumInfo().getElevation(), 0.0001);
        assertEquals(ts.getVerticalDatumInfo().getNativeDatum(), convertBackToOriginal.getVerticalDatumInfo().getNativeDatum());
        //verify all original offsets are present in round-trip conversion and values match
        for(VerticalDatumInfo.Offset offset : convertBackToOriginal.getVerticalDatumInfo().getOffsets())
        {
            VerticalDatum convertedBackToDatum = VerticalDatum.getVerticalDatum(offset.getToDatum());
            VerticalDatumInfo.Offset originalToDatum = ts.getVerticalDatumInfo().getOffsetForDatum(convertedBackToDatum);
            assertNotNull(originalToDatum, "Round-trip conversion resulted in missing to-datum: " + convertedBackToDatum);
            assertEquals(originalToDatum.getValue(), offset.getValue(), 0.0001);
        }
    }
}
