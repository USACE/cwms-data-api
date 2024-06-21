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
import cwms.cda.data.dto.LocationIdentifierTest;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

final class TurbineTest {

    @ParameterizedTest
    @CsvSource({Formats.JSON, Formats.JSONV1, Formats.DEFAULT})
    void testTurbineSerializationRoundTrip(String format) {
        Turbine turbine = buildTestTurbine();
        String serialized = Formats.format(Formats.parseHeader(format, Turbine.class), turbine);
        Turbine deserialized = Formats.parseContent(Formats.parseHeader(format, Turbine.class),
                serialized, Turbine.class);
        assertSame(turbine, deserialized);
    }

    @Test
    void testTurbineSerializationRoundTripFromFile() throws Exception {
        Turbine turbine = buildTestTurbine();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/turbine.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Turbine deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, Turbine.class),
                serialized, Turbine.class);
        assertSame(turbine, deserialized);
    }

    @Test
    void testValidate() {
        Location location = buildTestLocation();
        assertAll(() -> {
                    Turbine turbine = new Turbine.Builder().build();
                    assertThrows(FieldException.class, turbine::validate,
                            "Expected validate() to throw FieldException because Location field can't be null, but it didn't");
                }, () -> {
                    Turbine turbine = new Turbine.Builder().withLocation(location).build();
                    assertThrows(FieldException.class, turbine::validate,
                            "Expected validate() to throw FieldException because Project Id field can't be null, but it didn't");
                }
        );
    }

    private Turbine buildTestTurbine() {
        return new Turbine.Builder()
                .withLocation(buildTestLocation())
                .withProjectId(new LocationIdentifier.Builder()
                        .withOfficeId("LRD")
                        .withName("PROJECT")
                        .build())
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "TURBINE", ZoneId.of("UTC"),
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

    private static void assertSame(Turbine first, Turbine second) {
        assertAll(
                () -> LocationIdentifierTest.assertSame(first.getProjectId(), second.getProjectId()),
                () -> assertEquals(first.getLocation(), second.getLocation(), "Locations are not the same")
        );
    }
}
