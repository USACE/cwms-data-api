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

package cwms.cda.data.dto.watersupply;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.CwmsIdTest;
import cwms.cda.formatters.Formats;
import cwms.cda.data.dto.LookupType;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class WaterSupplyTest {

    private static final String OFFICE_ID = "SPK";

    @Test
    void testWaterSupplySerializationRoundTrip() {
        WaterSupply waterSupply = buildTestWaterSupply();
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterSupply.class), waterSupply);
        WaterSupply deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, WaterSupply.class), serialized, WaterSupply.class);
        assertSame(waterSupply, deserialized);
    }

    @Test
    void testWaterSupplySerializationRoundTripFromFile() throws Exception
    {
        WaterSupply waterSupply = buildTestWaterSupply();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/watersupply/watersupply.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        WaterSupply deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, WaterSupply.class), serialized, WaterSupply.class);
        assertSame(waterSupply, deserialized);
    }

    @Test
    void testValidate()
    {
        assertAll(() -> {
                WaterSupply waterSupply = buildTestWaterSupply();
                assertDoesNotThrow(waterSupply::validate);
            }, () -> {
                WaterSupply waterSupply = new WaterSupply.Builder()
                    .withContractName("Test Contract")
                    .build();
                assertThrows(FieldException.class, waterSupply::validate,
            "Expected validate() to throw FieldException for missing fields, but it didn't");
            }
        );
    }

    private WaterSupply buildTestWaterSupply ()
    {
        return new WaterSupply.Builder()
                .withContractName("Test Contract")
                .withContractNumber(1234)
                .withWaterUser("Test User")
                .withUser(new WaterUser("Test Entity",
                        new CwmsId.Builder()
                                .withName("Base Location")
                                .withOfficeId(OFFICE_ID)
                                .build(),
                        "Water Right test"))
                .withContract(new WaterUserContract.Builder()
                                .withWaterUserContractRef(new WaterUserContractRef(new WaterUser("Entity Test",
                                                new CwmsId.Builder()
                                                        .withName("Base Location")
                                                        .withOfficeId(OFFICE_ID)
                                                        .build(),
                                        "Water Right Test"),
                                "Contract Name Test"))
                                .withWaterSupplyContract(new LookupType.Builder()
                                        .withActive(true)
                                        .withDisplayValue("Display Value Test")
                                        .withOfficeId(OFFICE_ID)
                                        .withTooltip("Example Tooltip")
                                        .build())
                                .withContractEffectiveDate(new Date(1719854376))
                                .withContractExpirationDate(new Date(1719854376))
                                .withContractedStorage(1000.0)
                                .withInitialUseAllocation(20.5)
                                .withFutureUseAllocation(79.5)
                                .withStorageUnitsId("Test Storage Unit")
                                .withFutureUsePercentActivated(25.5)
                                .withTotalAllocPercentActivated(15.2)
                                .withPumpOutLocation(buildLocationType("Pump Out Location"))
                                .withPumpOutBelowLocation(buildLocationType("Pump Out Below Location"))
                                .withPumpInLocation(buildLocationType("Pump In Location"))
                                .build())
                .build();
    }

    private Location buildLocationType(String pumpLocation)
    {
        return new Location.Builder()
                .withLocationRef( new CwmsId.Builder()
                        .withName("Base Location")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withStateInitial("CA")
                .withActiveFlag(true)
                .withBoundingOfficeId(OFFICE_ID)
                .withDescription("Place for testing")
                .withElevation(120.0)
                .withCountyName("Sacramento")
                .withElevUnitId("m")
                .withHorizontalDatum("WGS84")
                .withLatitude(0.0)
                .withLongitude(0.0)
                .withLocationKindId("Test Locale")
                .withTypeOfLocation("PUMP")
                .withLongName("Full Location Name")
                .withMapLabel("Map location")
                .withNationId("US")
                .withNearestCity("Davis")
                .withPublishedLatitude(0.0)
                .withPublishedLongitude(0.0)
                .withTimeZoneName("UTC")
                .withBoundingOfficeName("Sacramento Office")
                .withVerticalDatum("WGS84")
                .withPublicName(pumpLocation)
                .build();
    }


    private static void assertSame(WaterSupply first, WaterSupply second)
    {
        assertAll(
                () -> assertEquals(first.getContractName(), second.getContractName()),
                () -> assertEquals(first.getContractNumber(), second.getContractNumber()),
                () -> assertEquals(first.getWaterUser(), second.getWaterUser()),
                () -> assertWaterUserType(first.getUser(), second.getUser()),
                () -> assertWaterUserContractType(first.getContract(), second.getContract())
        );
    }

    private static void assertLocationType(Location first, Location second)
    {
        assertAll(
                () -> assertEquals(first.getNearestCity(), second.getNearestCity()),
                () -> assertEquals(first.getNationId(), second.getNationId()),
                () -> assertEquals(first.getBoundingOfficeName(), second.getBoundingOfficeName()),
                () -> assertEquals(first.getBoundingOfficeId(), second.getBoundingOfficeId()),
                () -> assertEquals(first.getPublishedLongitude(), second.getPublishedLongitude()),
                () -> assertEquals(first.getPublishedLatitude(), second.getPublishedLatitude()),
                () -> assertEquals(first.getMapLabel(), second.getMapLabel()),
                () -> assertEquals(first.getLocationKindId(), second.getLocationKindId()),
                () -> assertEquals(first.getActiveFlag(), second.getActiveFlag()),
                () -> assertEquals(first.getDescription(), second.getDescription()),
                () -> assertEquals(first.getLongName(), second.getLongName()),
                () -> assertEquals(first.getPublicName(), second.getPublicName()),
                () -> assertEquals(first.getVerticalDatum(), second.getVerticalDatum()),
                () -> assertEquals(first.getElevUnitId(), second.getElevUnitId()),
                () -> assertEquals(first.getElevation(), second.getElevation()),
                () -> assertEquals(first.getHorizontalDatum(), second.getHorizontalDatum()),
                () -> assertEquals(first.getLongitude(), second.getLongitude()),
                () -> assertEquals(first.getLatitude(), second.getLatitude()),
                () -> assertEquals(first.getTypeOfLocation(), second.getTypeOfLocation()),
                () -> assertEquals(first.getTimeZoneName(), second.getTimeZoneName()),
                () -> assertEquals(first.getCountyName(), second.getCountyName()),
                () -> assertEquals(first.getStateInitial(), second.getStateInitial()),
                () -> CwmsIdTest.assertSame(first.getLocationRef(), second.getLocationRef())
        );
    }

    private static void assertLookupType(LookupType first, LookupType second)
    {
        assertAll(
                () -> assertEquals(first.getOfficeId(), second.getOfficeId()),
                () -> assertEquals(first.getActive(), second.getActive()),
                () -> assertEquals(first.getDisplayValue(), second.getDisplayValue()),
                () -> assertEquals(first.getTooltip(), second.getTooltip())
        );
    }

    private static void assertWaterUserType(WaterUser first, WaterUser second)
    {
        assertAll(
                () -> assertEquals(first.getEntityName(), second.getEntityName()),
                () -> CwmsIdTest.assertSame(first.getParentLocationRef(), second.getParentLocationRef()),
                () -> assertEquals(first.getWaterRight(), second.getWaterRight())
        );
    }

    private static void assertWaterUserContractType(WaterUserContract first, WaterUserContract second)
    {
        assertAll(
                () -> assertWaterUserContractRefType(first.getWaterUserContractRef(), second.getWaterUserContractRef()),
                () -> assertLookupType(first.getWaterSupplyContract(), second.getWaterSupplyContract()),
                () -> assertEquals(first.getContractEffectiveDate(), second.getContractEffectiveDate()),
                () -> assertEquals(first.getContractExpirationDate(), second.getContractExpirationDate()),
                () -> assertEquals(first.getContractedStorage(), second.getContractedStorage()),
                () -> assertEquals(first.getInitialUseAllocation(), second.getInitialUseAllocation()),
                () -> assertEquals(first.getFutureUseAllocation(), second.getFutureUseAllocation()),
                () -> assertEquals(first.getStorageUnitsId(), second.getStorageUnitsId()),
                () -> assertEquals(first.getFutureUsePercentActivated(), second.getFutureUsePercentActivated()),
                () -> assertEquals(first.getTotalAllocPercentActivated(), second.getTotalAllocPercentActivated()),
                () -> assertLocationType(first.getPumpOutLocation(), second.getPumpOutLocation()),
                () -> assertLocationType(first.getPumpOutBelowLocation(), second.getPumpOutBelowLocation()),
                () -> assertLocationType(first.getPumpInLocation(), second.getPumpInLocation())
        );
    }

    private static void assertWaterUserContractRefType(WaterUserContractRef first, WaterUserContractRef second)
    {
        assertAll(
                () -> assertEquals(first.getContractName(), second.getContractName()),
                () -> assertWaterUserType(first.getWaterUser(), second.getWaterUser())
        );
    }
}

