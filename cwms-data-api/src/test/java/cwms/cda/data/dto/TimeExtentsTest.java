/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class TimeExtentsTest {

    @Test
    void createTimeExtents_allFieldsProvided_success() {
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents extents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();

        assertAll(
                () -> assertEquals(start, extents.getEarliestTime(), "Start Instant"),
                () -> assertEquals(end, extents.getLatestTime(), "End Instant")
        );
    }

    @Test
    void createTimeExtents_serialize_roundtrip() {
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents extents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, extents);
        TimeExtents deserialized = Formats.parseContent(contentType, json, TimeExtents.class);

        DTOMatch.assertMatch(extents, deserialized);
    }

    @Test
    void createTimeExtents_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/time_extents.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        TimeExtents deserialized = Formats.parseContent(contentType, json, TimeExtents.class);
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents expectedExtents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();

        DTOMatch.assertMatch(expectedExtents, deserialized);
    }
}
