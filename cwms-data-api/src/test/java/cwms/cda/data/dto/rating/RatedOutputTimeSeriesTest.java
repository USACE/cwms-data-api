/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.rating;

import static cwms.cda.helpers.DTOMatch.assertMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class RatedOutputTimeSeriesTest {

    @Test
    void testSerializationRoundTrip() throws Exception {
        InputStream resource =
            this.getClass().getResourceAsStream("/cwms/cda/data/dto/rating/rated_output_timeseries.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        RatedOutputTimeSeries deserialized = Formats.parseContent(contentType, json, RatedOutputTimeSeries.class);
        CwmsId cwmsId = CwmsId.buildCwmsId("NWDP", "DOTW.Stage;Flow.Logarithmic.USGS-NWIS");
        List<TimeSeries.Record> depValues =
            Arrays.asList(new TimeSeries.StandardRecord(new Timestamp(1672531200000L), 137.90304290304002, 0),
                new TimeSeries.StandardRecord(new Timestamp(1577836800000L), 167.0693948928, 0),
                new TimeSeries.StandardRecord(new Timestamp(1546300800000L), null, 5),
                new TimeSeries.StandardRecord(new Timestamp(1451606400000L), 0.0269010042624, 0),
                new TimeSeries.StandardRecord(new Timestamp(1388534400000L), 1786.7930199552, 0));
        String outputUnit = "cfs";
        assertEquals(depValues, deserialized.getValues());
        assertMatch(cwmsId, deserialized.getRatingId());
        assertEquals(outputUnit, deserialized.getUnit());

    }
}
