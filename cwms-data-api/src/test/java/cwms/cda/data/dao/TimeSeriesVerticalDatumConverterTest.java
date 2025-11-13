package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
            assertEquals(convertedTs.getValues().get(i).getValue(), ts.getValues().get(i).getValue() + conversionFactor, 0.0001);
        }
    }
}
