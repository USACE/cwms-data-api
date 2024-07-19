/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package cwms.cda.data.dto.watersupply;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class WaterSupplyPumpAccountingTest {
    private static final String OFFICE = "SPK";

    @Test
    void testWaterSupplyPumpAccountingSerializationRoundTrip() {
        WaterUser user = new WaterUser("Test Entity", new CwmsId.Builder()
            .withOfficeId(OFFICE)
            .withName("Test Location")
            .build(),
            "Test Water Right");

        WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(user, "Test Contract",
            new Location.Builder(OFFICE, "NAME").withLocationType("PUMP").withNation(Nation.US)
                .withPublicName("PUMP 1").withLatitude(56.8).withLongitude(31.9).withDescription("PUMP")
                .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGSV84").build(),
            new LookupType.Builder().withActive(true).withTooltip("Test transfer Tip").withOfficeId(OFFICE)
                .withDisplayValue("Transfer").build(), 1.0, new Date(), "Test Comment");
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterSupplyPumpAccounting.class),
            waterSupplyPumpAccounting);
        WaterSupplyPumpAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
            WaterSupplyPumpAccounting.class), serialized, WaterSupplyPumpAccounting.class);
        DTOMatch.assertMatch(waterSupplyPumpAccounting, deserialized);
    }

    @Test
    void testWaterSupplyPumpAccountingSerializationRoundTripFromFile() throws Exception {
        WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
            new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
            "Test Contract",
            new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                .withStateInitial("OR").withCountyName("Test County").build(),
            new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                .withDisplayValue("Test Transfer Type").build(), 1.0, new Date(10000012648112L), "Test Comment");
        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/data/dto/watersupply/water_supply_accounting.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        WaterSupplyPumpAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                WaterSupplyPumpAccounting.class), serialized, WaterSupplyPumpAccounting.class);
        DTOMatch.assertMatch(waterSupplyPumpAccounting, deserialized);
    }


    @Test
    void testValidate() {
        assertAll(
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    "Test Contract",
                    new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                        .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                        .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                        .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                        .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                        .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                        .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                        .withStateInitial("OR").withCountyName("Test County").build(),
                    new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                        .withDisplayValue("Test Transfer Type").build(), 1.0, new Date(10000012648112L), "Test Comment");
                assertDoesNotThrow(waterSupplyPumpAccounting::validate, "Expected validation to pass");
            },
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    null,
                    new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                        .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                        .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                        .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                        .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                        .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                        .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                        .withStateInitial("OR").withCountyName("Test County").build(),
                    new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                        .withDisplayValue("Test Transfer Type").build(), 1.0, new Date(10000012648112L), "Test Comment");
                assertThrows(FieldException.class, waterSupplyPumpAccounting::validate, "Expected validation to "
                    + "fail due to null contract name");
            },
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    "Test Contract",
                    null,
                    new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                        .withDisplayValue("Test Transfer Type").build(), 1.0, new Date(10000012648112L), "Test Comment");
                assertThrows(FieldException.class, waterSupplyPumpAccounting::validate, "Expected validation to "
                    + "fail due to null location");
            },
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    "Test Contract",
                    new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                        .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                        .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                        .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                        .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                        .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                        .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                        .withStateInitial("OR").withCountyName("Test County").build(),
                    null, 1.0, new Date(10000012648112L), "Test Comment");
                assertThrows(FieldException.class, waterSupplyPumpAccounting::validate, "Expected validation to "
                    + "fail due to null transfer type");
            },
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    null,
                    new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                        .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                        .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                        .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                        .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                        .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                        .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                        .withStateInitial("OR").withCountyName("Test County").build(),
                    new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                        .withDisplayValue("Test Transfer Type").build(), null, new Date(10000012648112L), "Test Comment");
                assertThrows(FieldException.class, waterSupplyPumpAccounting::validate, "Expected validation to "
                    + "fail due to null flow value");
            },
            () -> {
                WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting(new WaterUser("Test Entity",
                    new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location").build(), "Test Water Right"),
                    null,
                    new Location.Builder(OFFICE, "Test Pump").withLocationType("PUMP").withNation(Nation.US)
                        .withPublicName("Test Public Name").withLatitude(45.0).withLongitude(-120.0).withDescription("PUMP")
                        .withTimeZoneName(ZoneId.of("UTC")).withLocationKind("PUMP").withHorizontalDatum("WGS84")
                        .withElevation(1000.0).withMapLabel("Test Map Label").withBoundingOfficeId("SPK")
                        .withElevationUnits("m").withActive(true).withTimeZoneName(ZoneId.of("UTC"))
                        .withVerticalDatum("NGVD29").withDescription("Test Description").withLocationKind("PUMP")
                        .withPublishedLatitude(45.0).withPublishedLongitude(-120.0).withLongName("Test Long Name")
                        .withStateInitial("OR").withCountyName("Test County").build(),
                    new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                        .withDisplayValue("Test Transfer Type").build(), 1.0, null, "Test Comment");
                assertThrows(FieldException.class, waterSupplyPumpAccounting::validate, "Expected validation to "
                    + "fail due to null transfer date");
            }
        );
    }

}
