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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/**
 *
 */
final class RateInputTimeSeriesTest {

    @Test
    void testGetValues() {
        List<String> testTimeSeriesIds = Arrays.asList(
            "CGR-RO1.Opening.Inst.0.0.CENWP-Manual-Raw",
            "CGR-RO1-Bypass.Elev.Inst.0.0.GDACS-RAW"
        );
        RateInputTimeSeries rateInputValues = new RateInputTimeSeries.RateInputTimeSeriesBuilder()
            .withTimeSeriesIds(testTimeSeriesIds)
            .withOutputUnit("cfs")
            .build();
        rateInputValues.validate();
        assertNotNull(rateInputValues.getTimeSeriesIds(), "getValues should not return null.");
        assertEquals(testTimeSeriesIds, rateInputValues.getTimeSeriesIds(), "getValues should return the correct list of values.");
    }

    @Test
    void testBuilderWithInvalidValuesThrowsException() {
        assertThrows(RequiredFieldException.class, () -> {
            new RateInputTimeSeries.RateInputTimeSeriesBuilder()
                .withTimeSeriesIds(null)
                .withOutputUnit("cfs")
                .withStartTime(Instant.now().toEpochMilli())
                .withEndTime(Instant.now().toEpochMilli())
                .build()
                .validate();
        }, "Builder should throw an exception when time series ids are null.");
        assertThrows(RequiredFieldException.class, () -> {
            new RateInputTimeSeries.RateInputTimeSeriesBuilder()
                .withTimeSeriesIds(new ArrayList<>())
                .withOutputUnit("cfs")
                .withStartTime(Instant.now().toEpochMilli())
                .withEndTime(Instant.now().toEpochMilli())
                .build()
                .validate();
        }, "Builder should throw an exception when time series ids are empty.");
        assertThrows(RequiredFieldException.class, () -> {
            new RateInputTimeSeries.RateInputTimeSeriesBuilder()
                .withTimeSeriesIds(Collections.singletonList("CGR-RO1.Opening.Inst.0.0.CENWP-Manual-Raw"))
                .withOutputUnit("cfs")
                .withStartTime(null)
                .withEndTime(Instant.now().toEpochMilli())
                .build()
                .validate();
        }, "Builder should throw an exception when start time is null.");
        assertThrows(RequiredFieldException.class, () -> {
            new RateInputTimeSeries.RateInputTimeSeriesBuilder()
                .withTimeSeriesIds(Collections.singletonList("CGR-RO1.Opening.Inst.0.0.CENWP-Manual-Raw"))
                .withOutputUnit("cfs")
                .withStartTime(Instant.now().toEpochMilli())
                .withEndTime(null)
                .build()
                .validate();
        }, "Builder should throw an exception when end time is null.");
    }

    @Test
    void testBuilderWithEmptyValuesThrowsException() {
        List<String> invalidTimeSeriesIds = new ArrayList<>();
        assertThrows(FieldException.class, () -> {
            new RateInputTimeSeries.RateInputTimeSeriesBuilder()
                .withTimeSeriesIds(invalidTimeSeriesIds)
                .withOutputUnit("cfs")
                .build()
                .validate();
        }, "Builder should throw an exception when values are empty.");
    }


    @Test
    void testSerializationRoundTrip() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/rating/rate_input_timeseries.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        RateInputTimeSeries deserialized = Formats.parseContent(contentType, json, RateInputTimeSeries.class);
        assertEquals("cfs", deserialized.getOutputUnit(), "Output unit should match.");
        assertEquals(Instant.ofEpochMilli(1672531200000L), deserialized.getRatingTime().get(),
            "Rating time should match.");
        assertTrue(deserialized.getRound(), "Round should match.");
        assertEquals(Arrays.asList("CGR-RO1.Opening.Inst.0.0.CENWP-Manual-Raw",
            "CGR-RO1-Bypass.Elev.Inst.0.0.GDACS-RAW"), deserialized.getTimeSeriesIds(), "Timeseries ids should match.");
        assertEquals(Instant.ofEpochMilli(1672531200000L),
            deserialized.getStartTime(), "Output unit should match.");
        assertEquals(Instant.ofEpochMilli(1388534400000L),
            deserialized.getEndTime(), "Output unit should match.");
        assertEquals(Instant.ofEpochMilli(1451606400000L),
            deserialized.getVersionDate().get(), "Output unit should match.");
        assertTrue(deserialized.getTrim(), "Trim should match.");
        assertFalse(deserialized.getStartInclusive(), "Start inclusive should match.");
        assertFalse(deserialized.getEndInclusive(), "End inclusive should match.");
        assertTrue(deserialized.getPrevious(), "Previous should match.");
        assertTrue(deserialized.getNext(), "Next should match.");

    }
}
