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

final class CwmsIdTimeExtentsEntryTest {

    @Test
    void createCwmsIdTimeExtentsEntry_allFieldsProvided_success() {
        CwmsId id = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("ARBU").build();
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents timeExtents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();
        CwmsIdTimeExtentsEntry entry = new CwmsIdTimeExtentsEntry.Builder()
                .withId(id)
                .withTimeExtents(timeExtents)
                .build();

        assertAll(
                () -> assertEquals(id, entry.getId(), "CwmsId"),
                () -> assertEquals(timeExtents, entry.getTimeExtents(), "TimeExtents")
        );
    }

    @Test
    void createCwmsIdTimeExtentsEntry_serialize_roundtrip() {
        CwmsId id = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("ARBU").build();
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents timeExtents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();
        CwmsIdTimeExtentsEntry entry = new CwmsIdTimeExtentsEntry.Builder()
                .withId(id)
                .withTimeExtents(timeExtents)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, entry);
        CwmsIdTimeExtentsEntry deserialized = Formats.parseContent(contentType, json, CwmsIdTimeExtentsEntry.class);

        DTOMatch.assertMatch(entry, deserialized);
    }

    @Test
    void createCwmsIdTimeExtentsEntry_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/cwms_id_time_extents_entry.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        CwmsIdTimeExtentsEntry deserialized = Formats.parseContent(contentType, json, CwmsIdTimeExtentsEntry.class);

        CwmsId id = new CwmsId.Builder()
                .withOfficeId("SWT")
                .withName("ARBU").build();
        ZonedDateTime start = ZonedDateTime.parse("2024-12-06T00:00:00Z");
        ZonedDateTime end = ZonedDateTime.parse("2024-12-06T12:00:00Z");
        TimeExtents timeExtents = new TimeExtents.Builder()
                .withEarliestTime(start)
                .withLatestTime(end)
                .build();
        CwmsIdTimeExtentsEntry expectedEntry = new CwmsIdTimeExtentsEntry.Builder()
                .withId(id)
                .withTimeExtents(timeExtents)
                .build();

        DTOMatch.assertMatch(expectedEntry, deserialized);
    }
}
