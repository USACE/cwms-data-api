/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

final class CwmsIdLocationKindTest {

    @Test
    void test_CreateCwmsIdLocationKind_RoundTrip() {
        // Test the creation of CwmsIdLocationKind instances
        String office = "SPK";
        String locationId = "Pine Flat Area";
        String kindName = "Basin";
        CwmsIdLocationKind kind = new CwmsIdLocationKind.Builder()
                .withLocationId(CwmsId.buildCwmsId(office, locationId))
                .withLocationKindId(kindName).build();

        ContentType contentType = new ContentType(Formats.JSONV1);
        String json = Formats.format(contentType, kind);

        CwmsIdLocationKind parsedKind = Formats.parseContent(contentType, json, CwmsIdLocationKind.class);

        // Verify that the parsed kind matches the original
        DTOMatch.assertMatch(kind.getLocationId(), parsedKind.getLocationId());
        assertEquals(kind.getLocationKindId(), parsedKind.getLocationKindId());
    }

    @Test
    void test_DeserializeFromFile() {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/cwmsid_location_kind.json");
        ContentType contentType = new ContentType(Formats.JSONV1);
        CwmsIdLocationKind parsedKind = Formats.parseContent(contentType, resource, CwmsIdLocationKind.class);

        assertEquals("Pine Flat Area", parsedKind.getLocationId().getName());
        assertEquals("Outlet", parsedKind.getLocationKindId());
        assertEquals("SPK", parsedKind.getLocationId().getOfficeId());
    }
}
