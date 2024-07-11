/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class VirtualOutletTest {
    private static final String SPK = "SPK";

    @Test
    void test_serialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        VirtualOutlet data = buildTestData();
        String json = Formats.format(contentType, data);

        VirtualOutlet deserialized = Formats.parseContent(contentType, json, VirtualOutlet.class);
        DTOMatch.assertMatch(data, deserialized);
    }

    @Test
    void test_serialize_from_file() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        VirtualOutlet data = buildTestData();
        InputStream resource = this.getClass()
                                   .getResourceAsStream("/cwms/cda/data/dto/location/kind/virtual-outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        VirtualOutlet deserialized = Formats.parseContent(contentType, serialized, VirtualOutlet.class);
        DTOMatch.assertMatch(data, deserialized);
    }

    private VirtualOutlet buildTestData() {
        CwmsId.Builder builder = new CwmsId.Builder().withOfficeId(SPK);
        return new VirtualOutlet.Builder().withProjectId(builder.withName("BIGH").build())
                                          .withVirtualOutletId(builder.withName("Compound Tainter Gates").build())
                                          .withVirtualRecords(
                                                  Arrays.asList(buildRecord("TG1", "TG2"), buildRecord("TG2", "TG3"),
                                                                buildRecord("TG3", "TG4", "TG5"), buildRecord("TG4"),
                                                                buildRecord("TG5")))
                                          .build();
    }

    private VirtualOutletRecord buildRecord(String upstream, String ... downstream) {
        CwmsId.Builder builder = new CwmsId.Builder().withOfficeId(SPK);
        List<CwmsId> downstreamIds = Arrays.stream(downstream)
                                     .map(id -> builder.withName(id).build())
                                     .collect(Collectors.toList());
        return new VirtualOutletRecord.Builder().withOutletId(builder.withName(upstream).build())
                                                .withDownstreamOutletIds(downstreamIds)
                                                .build();
    }
}
