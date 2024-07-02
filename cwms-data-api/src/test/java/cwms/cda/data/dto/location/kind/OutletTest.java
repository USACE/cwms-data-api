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
import cwms.cda.data.dto.CwmsIdTest;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OutletTest {
    private static final String SPK = "SPK";
    private static final String PROJECT_LOC = "location";
    private static final String OUTLET_LOC = PROJECT_LOC + "-outlet";
    private static final String BASE_OUTLET_LOC = "outlet";

    @Test
    void test_serialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(OUTLET_LOC);
        String json = Formats.format(contentType, outlet);

        Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
        assertEquals(outlet.getLocation(), parsedOutlet.getLocation(), "Locations do not match");
        CwmsIdTest.assertSame(outlet.getProjectId(), parsedOutlet.getProjectId());
    }

    @Test
    void test_serialization_base_loc_only() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(BASE_OUTLET_LOC);
        String json = Formats.format(contentType, outlet);

        Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
        assertEquals(outlet.getLocation(), parsedOutlet.getLocation(), "Locations do not match");
        CwmsIdTest.assertSame(outlet.getProjectId(), parsedOutlet.getProjectId());
    }

    @Test
    void test_serialize_from_file() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(OUTLET_LOC);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
        assertEquals(outlet.getLocation(), deserialized.getLocation(), "Locations do not match");
        CwmsIdTest.assertSame(outlet.getProjectId(), deserialized.getProjectId(), "project-id");
    }

    @Test
    void test_serialize_from_file_base_loc_only() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(BASE_OUTLET_LOC);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/base_outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
        assertEquals(outlet.getLocation(), deserialized.getLocation(), "Locations do not match");
        CwmsIdTest.assertSame(outlet.getProjectId(), deserialized.getProjectId(), "project-id");
    }

    private Outlet buildTestOutlet(String outletLocId) {
        CwmsId identifier = new CwmsId.Builder()
                .withName(PROJECT_LOC)
                .withOfficeId(SPK)
                .build();
        Location loc = new Location.Builder(SPK, outletLocId)
                .withLatitude(0.)
                .withLongitude(0.)
                .withPublicName(outletLocId)
                .withLocationKind("Outlet")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("NAD84")
                .withVerticalDatum("NAVD88")
                .build();

        return new Outlet.Builder()
                .withProjectId(identifier)
                .withLocation(loc)
                .build();
    }
}