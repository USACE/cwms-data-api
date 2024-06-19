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

package cwms.cda.data.dto.location.kind;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

final class EmbankmentTest {

    @Test
    void testEmbankmentSerializationRoundTrip() {
        Embankment embankment = buildTestEmbankment();
        String serialized = Formats.format(Formats.parseHeader(Formats.JSON), embankment);
        Embankment deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSON), serialized, Embankment.class);
        assertEquals(embankment, deserialized, "Roundtrip serialization failed");
        assertEquals(embankment.hashCode(), deserialized.hashCode(), "Roundtrip serialization failed");
    }

    @Test
    void testEmbankmentSerializationRoundTripFromFile() throws Exception {
        Embankment embankment = buildTestEmbankment();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/embankment.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Embankment deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSON), serialized, Embankment.class);
        assertEquals(embankment, deserialized, "Roundtrip serialization failed");
    }

    @Test
    void testValidate() {
        Location location = buildTestLocation();
        assertAll(() -> {
                    Embankment embankment = new Embankment.Builder().build();
                    assertThrows(FieldException.class, embankment::validate,
                            "Expected validate() to throw FieldException because Location field can't be null, but it didn't");
                }, () -> {
                    Embankment embankment = new Embankment.Builder().withLocation(location).build();
                    assertThrows(FieldException.class, embankment::validate,
                            "Expected validate() to throw FieldException because Project Id field can't be null, but it didn't");
                }
        );
    }

    private Embankment buildTestEmbankment() {
        return new Embankment.Builder()
                .withLocation(buildTestLocation())
                .withHeightMax(5.0)
                .withProjectIdentifier(new LocationIdentifier.Builder()
                        .withOfficeId("LRD")
                        .withLocationId("PROJECT")
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
