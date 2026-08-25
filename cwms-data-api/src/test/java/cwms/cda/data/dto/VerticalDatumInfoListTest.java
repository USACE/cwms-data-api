/*
 *
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static cwms.cda.data.dao.JsonRatingUtilsTest.readFully;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class VerticalDatumInfoListTest {

    @Test
    void test_roundtrip() throws Exception {
        List<VerticalDatumInfo> list = new ArrayList<>();
        VerticalDatumInfo info = new VerticalDatumInfo.Builder()
            .withElevation(100.0)
            .withNativeDatum("NGVD-29")
            .withUnit("m")
            .withLocation("VDI_LOC_TEST")
            .withOffice("SPK")
            .withOffset(true, "NAVD-88", -0.5)
            .build();
        list.add(info);
        VerticalDatumInfo info2 = new VerticalDatumInfo.Builder()
            .withElevation(200.0)
            .withNativeDatum("NGVD-29")
            .withUnit("m")
            .withOffice("SPK")
            .withLocation("VDI_LOC_TEST2")
            .withOffset(true, "NAVD-88", -0.5)
            .build();
        list.add(info2);
        VerticalDatumInfoList vdiList = new VerticalDatumInfoList(list);
        String serialized = Formats.format(new ContentType(Formats.XML), vdiList);
        InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/vertical-datum-bulk.xml");
        assertNotNull(stream);
        String expected = readFully(stream);
        VerticalDatumInfoList deserializedFromFile = Formats.parseContent(new ContentType(Formats.XML), expected, VerticalDatumInfoList.class);
        VerticalDatumInfoList deserialized = Formats.parseContent(new ContentType(Formats.XML), serialized, VerticalDatumInfoList.class);
        for (VerticalDatumInfo vdi : deserialized.getDatumList()) {
            assertTrue(vdi.equals(info) || vdi.equals(info2));
        }
        for (VerticalDatumInfo vdi : deserializedFromFile.getDatumList()) {
            assertTrue(vdi.equals(info) || vdi.equals(info2));
        }
        assertEquals(2, deserialized.getDatumList().size());
        assertEquals(2, deserializedFromFile.getDatumList().size());
    }
}
