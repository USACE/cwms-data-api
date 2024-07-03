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
import cwms.cda.data.dto.CwmsIdTest;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupTypeTest;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import cwms.cda.data.dto.LookupType;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

final class WaterUserContractTest {
    private static final String OFFICE_ID = "MVR";

    @Test
    void testWaterUserContractSerializationRoundTrip() {
        WaterUserContract waterUserContract = buildTestWaterUserContract();
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class),
                waterUserContract);
        WaterUserContract deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                WaterUserContract.class), serialized, WaterUserContract.class);
        assertSame(waterUserContract, deserialized);
    }

    @Test
    void testWaterUserContractSerializationRoundTripFromFile() throws Exception {
        WaterUserContract waterUserContract = buildTestWaterUserContract();
        InputStream resource = this.getClass()
                .getResourceAsStream("/cwms/cda/data/dto/watersupply/waterusercontract.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        WaterUserContract deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                WaterUserContract.class), serialized, WaterUserContract.class);
        assertSame(waterUserContract, deserialized);
    }

    @Test
    void testValidate() {
        assertAll(
                () -> {
                    WaterUserContract waterUserContract = buildTestWaterUserContract();
                    assertDoesNotThrow(waterUserContract::validate,
                            "Expected validation to pass without errors");
                },
                () -> {
                    WaterUserContract waterUserContract = new WaterUserContract.Builder()
                            .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Test User",
                                    new CwmsId.Builder().withName("TEST_LOCATION1").withOfficeId(OFFICE_ID).build(),
                                    "Test Water Right"), "Test Contract"))
                            .withWaterContract(new LookupType.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withActive(true)
                                    .withTooltip("TEST TOOLTIP")
                                    .withDisplayValue("Test Display Value")
                                    .build())
                            .withContractEffectiveDate(new Date(158000))
                            .withContractExpirationDate(new Date(167000))
                            .withFutureUseAllocation(27800.5)
                            .withStorageUnitsId("%")
                            .withContractedStorage(200000.5)
                            .withFutureUsePercentActivated(15.6)
                            .withTotalAllocPercentActivated(65.2)
                            .build();
                    assertThrows(FieldException.class, waterUserContract::validate,
                            "Expected validation to fail with null Initial Use Allocation");
                },
                () -> {
                    WaterUserContract waterUserContract = new WaterUserContract.Builder()
                            .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Test User",
                                    new CwmsId.Builder().withName("TEST_LOCATION1").withOfficeId(OFFICE_ID).build(),
                                    "Test Water Right"), "Test Contract"))
                            .withWaterContract(new LookupType.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withActive(true)
                                    .withTooltip("TEST TOOLTIP")
                                    .withDisplayValue("Test Display Value")
                                    .build())
                            .withContractEffectiveDate(new Date(158000))
                            .withContractExpirationDate(new Date(167000))
                            .withFutureUseAllocation(27800.5)
                            .withStorageUnitsId("%")
                            .withContractedStorage(200000.5)
                            .withInitialUseAllocation(15600.0)
                            .withFutureUsePercentActivated(15.6)
                            .build();
                    assertThrows(FieldException.class, waterUserContract::validate,
                            "Expected validation to fail with null Total Activated Allocation Percentage");
                },
                () -> {
                    WaterUserContract waterUserContract = new WaterUserContract.Builder()
                            .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Test User",
                                    new CwmsId.Builder().withName("TEST_LOCATION1").withOfficeId(OFFICE_ID).build(),
                                    "Test Water Right"), "Test Contract"))
                            .withWaterContract(new LookupType.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withActive(true)
                                    .withTooltip("TEST TOOLTIP")
                                    .withDisplayValue("Test Display Value")
                                    .build())
                            .withContractEffectiveDate(new Date(158000))
                            .withFutureUseAllocation(27800.5)
                            .withStorageUnitsId("%")
                            .withContractedStorage(200000.5)
                            .withInitialUseAllocation(15600.0)
                            .withFutureUsePercentActivated(15.6)
                            .withTotalAllocPercentActivated(65.2)
                            .build();
                    assertThrows(FieldException.class, waterUserContract::validate,
                            "Expected validation to fail with null Contract Expiration date");
                },
                () -> {
                    WaterUserContract waterUserContract = new WaterUserContract.Builder()
                            .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Test User",
                                    new CwmsId.Builder().withName("TEST_LOCATION1").withOfficeId(OFFICE_ID).build(),
                                    "Test Water Right"), "Test Contract"))
                            .withWaterContract(new LookupType.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withActive(true)
                                    .withTooltip("TEST TOOLTIP")
                                    .withDisplayValue("Test Display Value")
                                    .build())
                            .withContractEffectiveDate(new Date(158000))
                            .withContractExpirationDate(new Date(167000))
                            .withFutureUseAllocation(27800.5)
                            .withContractedStorage(200000.5)
                            .withInitialUseAllocation(15600.0)
                            .withFutureUsePercentActivated(15.6)
                            .withTotalAllocPercentActivated(65.2)
                            .build();
                    assertThrows(FieldException.class, waterUserContract::validate,
                            "Expected validation to fail with null Storage Units");
                },
                () -> {
                    WaterUserContract waterUserContract = new WaterUserContract.Builder()
                            .withWaterContract(new LookupType.Builder()
                                    .withOfficeId(OFFICE_ID)
                                    .withActive(true)
                                    .withTooltip("TEST TOOLTIP")
                                    .withDisplayValue("Test Display Value")
                                    .build())
                            .withContractEffectiveDate(new Date(158000))
                            .withContractExpirationDate(new Date(167000))
                            .withFutureUseAllocation(27800.5)
                            .withStorageUnitsId("%")
                            .withContractedStorage(200000.5)
                            .withInitialUseAllocation(15600.0)
                            .withFutureUsePercentActivated(15.6)
                            .withTotalAllocPercentActivated(65.2)
                            .build();
                    assertThrows(FieldException.class, waterUserContract::validate,
                            "Expected validation to fail with null User Contract Reference");
                }
        );

    }

    private static WaterUserContract buildTestWaterUserContract() {
        return new WaterUserContract.Builder()
                .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Test User",
                        new CwmsId.Builder().withName("TEST_LOCATION1").withOfficeId(OFFICE_ID).build(),
                        "Test Water Right"), "Test Contract"))
                .withWaterContract(new LookupType.Builder()
                        .withActive(true)
                        .withDisplayValue("Test Display Value")
                        .withOfficeId(OFFICE_ID)
                        .withTooltip("Test Tooltip")
                        .build())
                .withContractEffectiveDate(new Date(158000))
                .withContractExpirationDate(new Date(167000))
                .withInitialUseAllocation(15600.0)
                .withFutureUseAllocation(27800.5)
                .withStorageUnitsId("%")
                .withContractedStorage(200000.5)
                .withFutureUsePercentActivated(15.6)
                .withTotalAllocPercentActivated(65.2)
                .withPumpOutLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_OUT,
                        new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("PUMP1").build()))
                .withPumpOutBelowLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_OUT_BELOW,
                        new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("PUMP2").build()))
                .withPumpInLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_IN,
                        new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("PUMP3").build()))
                .build();
    }

    private static Location buildTestLocation() {
        return new Location.Builder(OFFICE_ID, "Test Location")
                .withDescription("Test Description")
                .withLocationType("Test Location Type")
                .withLatitude(0.0)
                .withLongName("Test Long Name")
                .withLongitude(0.0)
                .withHorizontalDatum("WGS84")
                .withLocationKind("PUMP")
                .withLocationType("Test Location Type")
                .withVerticalDatum("WGS84")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(true)
                .withPublicName("Test Public Pump Name")
                .withNation(Nation.US)
                .withStateInitial("NV")
                .withCountyName("Clark")
                .withNearestCity("Sparks")
                .withPublishedLongitude(0.0)
                .withPublishedLatitude(0.0)
                .withElevation(150.0)
                .withElevationUnits("m")
                .withMapLabel("Test Map Label")
                .withBoundingOfficeId(OFFICE_ID)
                .build();
    }

    public static void assertSame(WaterUserContract firstContract, WaterUserContract secondContract) {
        assertAll(
            () -> assertSame(firstContract.getWaterUserContractRef(), secondContract.getWaterUserContractRef()),
            () -> LookupTypeTest.assertSame(firstContract.getWaterContract(), secondContract.getWaterContract()),
            () -> assertEquals(firstContract.getContractEffectiveDate().toString(),
                    secondContract.getContractEffectiveDate().toString()),
            () -> assertEquals(firstContract.getContractExpirationDate().toString(),
                    secondContract.getContractExpirationDate().toString()),
            () -> assertEquals(firstContract.getContractedStorage(), secondContract.getContractedStorage()),
            () -> assertEquals(firstContract.getInitialUseAllocation(), secondContract.getInitialUseAllocation()),
            () -> assertEquals(firstContract.getFutureUseAllocation(), secondContract.getFutureUseAllocation()),
            () -> assertEquals(firstContract.getStorageUnitsId(), secondContract.getStorageUnitsId()),
            () -> assertEquals(firstContract.getFutureUsePercentActivated(),
                    secondContract.getFutureUsePercentActivated()),
            () -> assertEquals(firstContract.getTotalAllocPercentActivated(),
                    secondContract.getTotalAllocPercentActivated()),
            () -> assertSame(firstContract.getPumpOutLocation(), secondContract.getPumpOutLocation()),
            () -> assertSame(firstContract.getPumpOutBelowLocation(), secondContract.getPumpOutBelowLocation()),
            () -> assertSame(firstContract.getPumpInLocation(), secondContract.getPumpInLocation())
        );

    }

    private static void assertSame(WaterSupplyPump firstPump, WaterSupplyPump secondPump) {
        assertAll(
            () -> assertEquals(firstPump.getPumpLocation(), secondPump.getPumpLocation()),
            () -> assertEquals(firstPump.getPumpType(), secondPump.getPumpType()),
            () -> CwmsIdTest.assertSame(firstPump.getPumpId(), secondPump.getPumpId())
        );
    }

    private static void assertSame(WaterUserContractRef firstContractRef, WaterUserContractRef secondContractRef) {
        assertSame(firstContractRef.getWaterUser(), secondContractRef.getWaterUser());
        assertEquals(firstContractRef.getContractName(), secondContractRef.getContractName());
    }

    private static void assertSame(WaterUser firstUser, WaterUser secondUser) {
        assertAll(
            () -> assertEquals(firstUser.getEntityName(), secondUser.getEntityName()),
            () -> CwmsIdTest.assertSame(firstUser.getParentLocationRef(), secondUser.getParentLocationRef()),
            () -> assertEquals(firstUser.getWaterRight(), secondUser.getWaterRight())
        );
    }
}
