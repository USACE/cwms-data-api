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

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.udt.records.EMBANKMENT_OBJ_T;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class EmbankmentDaoTest {

    @Test
    void testJooqTypeConversion() {
        Embankment expected = buildTestEmbankment();
        EMBANKMENT_OBJ_T embankmentObjT = EmbankmentDao.map(expected);
        Embankment embankment = EmbankmentDao.map(embankmentObjT);
        assertEquals(expected, embankment, "Conversion of Embankment to jOOQ type and back failed.");
    }

    private Embankment buildTestEmbankment() {
        return new Embankment.Builder()
                .withLocation(buildTestLocation())
                .withHeightMax(5.0)
                .withProjectIdentifier(new LocationIdentifier.Builder()
                        .withLocationId("PROJECT")
                        .withOfficeId("LRD")
                        .build())
                .withStructureLength(10.0)
                .withStructureType(new LookupType.Builder()
                        .withOfficeId("CWMS")
                        .withDisplayValue("STRUCT")
                        .withTooltip("TOOLTIP_STRUCT")
                        .withActive(true)
                        .build())
                .withDownstreamProtType(new LookupType.Builder()
                        .withOfficeId("SPK")
                        .withDisplayValue("DOWNSTREAM_PROT")
                        .withTooltip("TOOLTIP_DOWNSTREAM_PROT")
                        .withActive(false)
                        .build())
                .withUpstreamProtType(new LookupType.Builder()
                        .withOfficeId("LRL")
                        .withDisplayValue("UPSTREAM_PROT")
                        .withTooltip("TOOLTIP_UPSTREAM_PROT")
                        .withActive(true)
                        .build())
                .withUpstreamSideSlope(15.0)
                .withUnitsId("ft")
                .withTopWidth(20.0)
                .withStructureLength(25.0)
                .withDownstreamSideSlope(90.0)
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "EMBANKMENT", ZoneId.of("UTC"),
                50.0, 50.0, "NVGD29", "LRL")
                .withElevation(10.0)
                .withElevationUnits("ft")
                .withLocationType("SITE")
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withBoundingOfficeId("LRL")
                .withLongName("TEST_LOCATION")
                .withPublishedLatitude(50.0)
                .withPublishedLongitude(50.0)
                .withDescription("for testing")
                .withNearestCity("Davis")
                .build();
    }
}
