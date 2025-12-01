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

package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TimeSeriesIdentifierDescriptorTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsID);
        assertNotNull(result);
        assertFalse(result.contains("aliases"));
    }

    @Test
    void testSerializeWithAliases() throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor tsID = buildTsId(buildAliases());

        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsID);
        assertNotNull(result);
        assertTrue(result.contains("aliases"));
        assertTrue(result.contains("alias"));
        assertTrue(result.contains("alias2"));
    }

    private TimeSeriesIdentifierDescriptor buildTsId() {
        return buildTsId(new ArrayList<>());
    }

    private TimeSeriesIdentifierDescriptor buildTsId(List<String> aliases) {
        TimeSeriesIdentifierDescriptor.Builder builder =
                new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withOfficeId("SWT");
        builder = builder.withTimeSeriesId("BASE-SUB.Area.Inst.1Hour.0.TEST");
        builder = builder.withZoneId(ZoneId.of("America/Los_Angeles"));
        builder = builder.withIntervalOffsetMinutes(0L);
        builder = builder.withActive(true);
        builder = builder.withAliases(aliases);

        return builder.build();
    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        jsonRoundtrip(tsID);

        // See if we can pass null for intervalOffsetMinutes
        TimeSeriesIdentifierDescriptor.Builder builder =
                new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withIntervalOffsetMinutes(null);
        jsonRoundtrip(builder.build());

        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withActive(false);
        jsonRoundtrip(builder.build());

        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withZoneId(null);
        jsonRoundtrip(builder.build());

        // Not sure if I should allow officeId to be null...
        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        builder.withOfficeId(null);
        jsonRoundtrip(builder.build());

    }

    private static void jsonRoundtrip(TimeSeriesIdentifierDescriptor tsID) throws JsonProcessingException {
        ObjectMapper om = JsonV2.buildObjectMapper();

        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();
        String result = ow.writeValueAsString(tsID);

        TimeSeriesIdentifierDescriptor tsID2 = om.readValue(result,
                TimeSeriesIdentifierDescriptor.class);

        assertNotNull(tsID2);

        assertEquals(tsID.getTimeSeriesId(), tsID2.getTimeSeriesId());
        assertEquals(tsID.getOfficeId(), tsID2.getOfficeId());
        assertEquals(tsID.getTimezoneName(), tsID2.getTimezoneName());
        assertEquals(tsID.getIntervalOffsetMinutes(), tsID2.getIntervalOffsetMinutes());
        assertEquals(tsID.isActive(), tsID2.isActive());

        assertEquals(tsID.getAliases().size(), tsID2.getAliases().size());
        if (!tsID.getAliases().isEmpty()) {
            for (int i = 0; i < tsID.getAliases().size(); i++) {
                assertEquals(tsID.getAliases().get(i), tsID2.getAliases().get(i));
            }
        }
    }

    @Test
    void jsonRoundtripWithAliases() throws JsonProcessingException {

        TimeSeriesIdentifierDescriptor tsID = buildTsId(buildAliases());
        assertFalse(tsID.getAliases().isEmpty());
        TimeSeriesIdentifierDescriptor.Builder builder =
            new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withTimeSeriesIdentifierDescriptor(tsID);
        jsonRoundtrip(builder.build());
    }

    private static List<String> buildAliases() {
        List<String> aliases = new ArrayList<>();
        aliases.add("alias");
        aliases.add("alias2");
        return aliases;
    }


    @Test
    void test_serialization_with_formats()
    {
        // This test verifies that the TimeSeriesIdentifierDescriptor can be serialized by the Formats class.
        // It will fail like:
        //  No Format for this content-type and data-type : (application/json;version=2, cwms.cda.data.dto.TimeSeriesIdentifierDescriptor)
        // If JsonV2 does not contain TimeSeriesIdentifierDescriptor in its list of classes
        TimeSeriesIdentifierDescriptor tsID = buildTsId();

        ContentType contentType = Formats.parseHeader(Formats.JSONV2, TimeSeriesIdentifierDescriptor.class);
        String jsonStr = Formats.format(contentType, tsID);
        assertNotNull(jsonStr);
    }

}