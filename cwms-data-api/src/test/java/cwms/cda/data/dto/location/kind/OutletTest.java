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
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class OutletTest {
    private static final String SPK = "SPK";
    private static final String PROJECT_LOC = "location";
    private static final String OUTLET_LOC = PROJECT_LOC + "-outlet";
    private static final String BASE_OUTLET_LOC = "outlet";
    private static final String COMPOUND_OUTLET_LOC1 = "CO1";
    private static final String RATING_GROUP_ID = "Rating-" + OUTLET_LOC;

    @Test
    void test_serialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(OUTLET_LOC);
        String json = Formats.format(contentType, outlet);

        Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
        DTOMatch.assertMatch(outlet, parsedOutlet);
    }

    @Test
    void test_serialization_base_loc_only() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(BASE_OUTLET_LOC);
        String json = Formats.format(contentType, outlet);

        Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
        DTOMatch.assertMatch(outlet, parsedOutlet);
    }

    @Test
    void test_serialize_from_file() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(OUTLET_LOC);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
        DTOMatch.assertMatch(outlet, deserialized);
    }

    @Test
    void test_serialize_from_file_base_loc_only() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(BASE_OUTLET_LOC);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/base_outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
        DTOMatch.assertMatch(outlet, deserialized);
    }

    @Test
    void test_serialize_compound_outlets() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(COMPOUND_OUTLET_LOC1,
                                        buildCompoundOutletRecord("TG1", SPK, "TG2"),
                                        buildCompoundOutletRecord("TG2", SPK, "TG3"),
                                        buildCompoundOutletRecord("TG3", SPK));
        String json = Formats.format(contentType, outlet);

        Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
        DTOMatch.assertMatch(outlet, parsedOutlet);
    }

    @Test
    void test_serialize_compound_outlets_from_file() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
        Outlet outlet = buildTestOutlet(COMPOUND_OUTLET_LOC1,
                                        buildCompoundOutletRecord("TG1", SPK, "TG2"),
                                        buildCompoundOutletRecord("TG2", SPK, "TG3"),
                                        buildCompoundOutletRecord("TG3", SPK, "TG4", "TG5"),
                                        buildCompoundOutletRecord("TG4", SPK),
                                        buildCompoundOutletRecord("TG5", SPK));
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/compound_outlet.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
        DTOMatch.assertMatch(outlet, deserialized);
    }

    private CompoundOutletRecord buildCompoundOutletRecord(String outletId, String officeId, String ... downstreamOutlets) {
        return new CompoundOutletRecord.Builder()
                .withOutletId(new CwmsId.Builder().withName(outletId).withOfficeId(officeId).build())
                .withDownstreamOutletIds(Arrays.stream(downstreamOutlets)
                                               .map(dso -> new CwmsId.Builder().withOfficeId(officeId)
                                                                               .withName(dso)
                                                                               .build())
                                               .collect(Collectors.toList()))
                .build();
    }

    private Outlet buildTestOutlet(String outletLocId, CompoundOutletRecord ... records) {
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

        return new Outlet.Builder().withProjectId(identifier)
                .withLocation(loc)
                .withRatingGroupId(RATING_GROUP_ID)
                .withCompoundOutletRecords(Arrays.asList(records))
                .build();
    }
}