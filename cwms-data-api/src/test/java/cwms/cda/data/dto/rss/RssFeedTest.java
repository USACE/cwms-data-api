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

package cwms.cda.data.dto.rss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class RssFeedTest {
    @Test
    void testSerialization() throws Exception {
        InputStream resource = getClass().getResourceAsStream("/cwms/cda/data/dto/rss/ts_stored.xml");
        assertNotNull(resource);
        String xmlOnDisk = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ContentType contentType = new ContentType(Formats.RSS);
        AtomLink next =
            new AtomLink("next", "https://localhost:7001/swt-data/rss/swt/ts_stored?page=fHx8fDUwMHx8MQ%3D%3D");
        List<RssItem> items = List.of(new RssItem(
            "{\"office_id\":\"SWT\",\"start_time\":1765033200000,\"ts_id\":\"AARK.Area.Inst.~1Day.0.TEST2\",\"ts_code\":263191,\"end_time\":1766847600000,\"store_time\":1765614311370,\"store_rule\":\"DELETE" +
                " INSERT\",\"version_date\":-27079747200000,\"type\":\"TSDataStored\",\"millis\":1765585516312}",
            Instant.parse("2025-12-13T00:25:19.600607Z").atZone(ZoneOffset.UTC), "45CB69B75AC67335E063400215ACD414"));
        RssChannel channel = new RssChannel("TS_STORED", next,
            "CWMS messages about time series operations, such as data stored and deleted", items);
        RssFeed rssFeed = new RssFeed(channel);
        String xmlSerialization = Formats.format(contentType, rssFeed);
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(xmlSerialization);
        JsonNode expected = xmlMapper.readTree(xmlOnDisk);
        assertEquals(expected.toPrettyString(), node.toPrettyString());
    }
}
