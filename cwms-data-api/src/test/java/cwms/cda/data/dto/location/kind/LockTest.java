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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class LockTest {

    @ParameterizedTest
    @CsvSource({Formats.JSON, Formats.JSONV1, Formats.DEFAULT})
    void testLockSerializationRoundTrip(String format) {
        Lock lock = buildTestLock();
        String serialized = Formats.format(Formats.parseHeader(format, Lock.class), lock);
        Lock deserialized = Formats.parseContent(Formats.parseHeader(format, Lock.class),
                serialized, Lock.class);
        DTOMatch.assertMatch(lock, deserialized);
    }

    @Test
    void testLockSerializationRoundTripFromFile() throws Exception {
        Lock lock = buildTestLock();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/lock.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Lock deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSON, Lock.class),
                serialized, Lock.class);
        DTOMatch.assertMatch(lock, deserialized);
    }

    @Test
    void testValidate() {
        Location location = buildTestLocation();
        assertAll(() -> {
                    Lock lock = new Lock.Builder().build();
                    assertThrows(FieldException.class, lock::validate,
                            "Expected validate() to throw FieldException because Location field can't be null, but it didn't");
                }, () -> {
                    Lock lock = new Lock.Builder().withLocation(location).build();
                    assertThrows(FieldException.class, lock::validate,
                            "Expected validate() to throw FieldException because Project Id field can't be null, but it didn't");
                }
        );
    }

    private Lock buildTestLock() {
        return new Lock.Builder()
                .withLocation(buildTestLocation())
                .withProjectId(new CwmsId.Builder()
                        .withOfficeId("LRD")
                        .withName("PROJECT")
                        .build())
                .withLockWidth(100.0)
                .withLockLength(100.0)
                .withNormalLockLift(10.0)
                .withMaximumLockLift(20.0)
                .withVolumePerLockage(100.0)
                .withMinimumDraft(5.0)
                .withUnits("ft")
                .withVolumeUnits("ft3")
                .withHighWaterLowerPoolWarningLevel(2)
                .withHighWaterUpperPoolWarningLevel(2)
                .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                        .withTooltip("CHAMBER").withDisplayValue("Land Side Main").build())
                .withHighWaterLowerPoolLocationLevel(new CwmsId.Builder()
                        .withName("HIGH_WATER_LOWER")
                        .withOfficeId("SPK")
                        .build())
                .withHighWaterUpperPoolLocationLevel(new CwmsId.Builder()
                        .withName("HIGH_WATER_UPPER")
                        .withOfficeId("SPK")
                        .build())
                .withLowWaterLowerPoolLocationLevel(new CwmsId.Builder()
                        .withName("LOW_WATER_LOWER")
                        .withOfficeId("SPK")
                        .build())
                .withLowWaterUpperPoolLocationLevel(new CwmsId.Builder()
                        .withName("LOW_WATER_UPPER")
                        .withOfficeId("SPK")
                        .build())
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "LOCK", ZoneId.of("UTC"),
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
