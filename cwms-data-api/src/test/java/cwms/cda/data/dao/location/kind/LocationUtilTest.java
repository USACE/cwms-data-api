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
import cwms.cda.data.dto.LookupType;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

final class LocationUtilTest {

    @Test
    void testLocationObjConversion() {
        Location expected = buildTestLocation();
        LOCATION_OBJ_T locationObj = LocationUtil.getLocation(expected);
        Location location = LocationUtil.getLocation(locationObj);
        assertEquals(expected, location, "Conversion of Location to jOOQ type and back failed.");
    }

    @Test
    void testLocationRefBaseSub() {
        LOCATION_REF_T locationRef = LocationUtil.getLocationRef("BASE-SUB", "SPK");
        assertAll(() -> assertEquals("SPK", locationRef.getOFFICE_ID()),
                () -> assertEquals("BASE", locationRef.getBASE_LOCATION_ID()),
                () -> assertEquals("SUB", locationRef.getSUB_LOCATION_ID()));
        assertEquals("BASE-SUB", LocationUtil.getLocationId(locationRef));
    }

    @Test
    void testLocationRefBaseOnly() {
        LOCATION_REF_T locationRef = LocationUtil.getLocationRef("BASE", "SPK");
        assertAll(() -> assertEquals("SPK", locationRef.getOFFICE_ID()),
                () -> assertEquals("BASE", locationRef.getBASE_LOCATION_ID()),
                () -> assertNull(locationRef.getSUB_LOCATION_ID()));
        assertEquals("BASE", LocationUtil.getLocationId(locationRef));
    }

    @Test
    void testLookupTypeObjConversion() {
        LookupType expected = buildTestLookupType();
        LOOKUP_TYPE_OBJ_T lookupTypeObjT = LocationUtil.getLookupType(expected);
        LookupType lookupType = LocationUtil.getLookupType(lookupTypeObjT);
        DTOMatch.assertMatch(expected, lookupType);
    }

    private LookupType buildTestLookupType() {
        return new LookupType.Builder()
                .withActive(true)
                .withDisplayValue("display")
                .withOfficeId("SPK")
                .withTooltip("tooltip")
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "EMBANKMENT", ZoneId.of("UTC"),
                50.0, 50.0, "NVGD29", "LRL")
                .withElevation(10.0)
                .withElevationUnits("m")
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
                .build();
    }
}
