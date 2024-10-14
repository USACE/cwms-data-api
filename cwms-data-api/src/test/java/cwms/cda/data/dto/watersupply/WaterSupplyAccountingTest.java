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

import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


class WaterSupplyAccountingTest {
    private static final String OFFICE = "SPK";

    @Test
    void testWaterSupplyAccountingSerializationRoundTrip() {
        WaterUser user = new WaterUser.Builder().withEntityName("California Department of Water Resources")
            .withProjectId(new CwmsId.Builder()
                .withOfficeId(OFFICE)
                .withName("Sacramento River Delta")
                .build())
            .withWaterRight("State of California Water Rights Permit #12345").build();
        WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                .withWaterUser(user).withContractName("Sacramento River Water Contract").withPumpLocations(
                    new PumpLocation.Builder()
                        .withPumpIn(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 1").build())
                        .withPumpOut(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 2").build())
                        .withPumpBelow(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 3").build())
                        .build())
                .withPumpAccounting(buildTestPumpAccountingList()).build();
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterSupplyAccounting.class),
            waterSupplyAccounting);
        WaterSupplyAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
            WaterSupplyAccounting.class), serialized, WaterSupplyAccounting.class);
        DTOMatch.assertMatch(waterSupplyAccounting, deserialized);
    }

    @Test
    void testWaterSupplyAccountingSerializationRoundTripFromFile() throws Exception {
        WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                .withWaterUser(new WaterUser.Builder()
                .withEntityName("California Department of Water Resources")
                .withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                        .withName("Sacramento River Delta").build())
                .withWaterRight("State of California Water Rights Permit #12345").build())
                .withContractName("Sacramento River Water Contract")
                .withPumpLocations(buildTestPumpLocation())
                .withPumpAccounting(buildTestPumpAccountingList())
                .build();
        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/data/dto/watersupply/water_supply_accounting.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        WaterSupplyAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                WaterSupplyAccounting.class), serialized, WaterSupplyAccounting.class);
        DTOMatch.assertMatch(waterSupplyAccounting, deserialized);
    }


    @Test
    void testValidate() {
        assertAll(
            () -> {
                WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                        .withWaterUser(new WaterUser.Builder()
                                .withEntityName("California Department of Water Resources")
                                .withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                                .withName("Sacramento River Delta").build())
                        .withWaterRight("State of California Water Rights Permit #12345").build())
                        .withContractName("Sacramento River Water Contract")
                        .withPumpAccounting(buildTestPumpAccountingList())
                        .withPumpLocations(buildTestPumpLocation())
                        .build();
                assertDoesNotThrow(waterSupplyAccounting::validate, "Expected validation to pass");
            },
            () -> {
                WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                        .withWaterUser(new WaterUser.Builder()
                        .withEntityName("California Department of Water Resources")
                                .withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                                .withName("Sacramento River Delta").build())
                        .withWaterRight("State of California Water Rights Permit #12345").build())
                        .withContractName(null)
                        .build();
                assertThrows(FieldException.class, waterSupplyAccounting::validate, "Expected validation to "
                    + "fail due to null contract name");
            },
            () -> {
                WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                        .withContractName("Sacramento River Water Contract")
                        .build();
                assertThrows(FieldException.class, waterSupplyAccounting::validate, "Expected validation to "
                    + "fail due to null water user");
            }
        );
    }


    private Map<Instant, List<PumpTransfer>> buildTestPumpAccountingList() {
        Map<Instant, List<PumpTransfer>> retMap = new TreeMap<>();
        List<PumpTransfer> pumpMap = new ArrayList<>();
        pumpMap.add(new PumpTransfer(PumpType.IN, "Pipeline", 1.0, "Added water to the system"));
        pumpMap.add(new PumpTransfer(PumpType.OUT, "Pipeline", 2.0, "Removed excess water"));
        pumpMap.add(new PumpTransfer(PumpType.BELOW, "River", 3.0, "Daily water release"));
        retMap.put(Instant.ofEpochMilli(1668979048000L), pumpMap);
        pumpMap = new ArrayList<>();
        pumpMap.add(new PumpTransfer(PumpType.IN, "Pipeline", 4.0, "Pump transfer for the day"));
        pumpMap.add(new PumpTransfer(PumpType.OUT, "Pipeline", 5.0, "Excess water transfer"));
        pumpMap.add(new PumpTransfer(PumpType.BELOW, "River", 6.0, "Water returned to the river"));
        retMap.put(Instant.ofEpochMilli(1669065448000L), pumpMap);
        pumpMap = new ArrayList<>();
        pumpMap.add(new PumpTransfer(PumpType.IN,"Pipeline", 7.0, "Pump transfer for the day"));
        pumpMap.add(new PumpTransfer(PumpType.OUT, "Pipeline", 8.0, "Excess water transfer"));
        pumpMap.add(new PumpTransfer(PumpType.BELOW, "River", 9.0, "Water returned to the river"));
        retMap.put(Instant.ofEpochMilli(1669151848000L), pumpMap);
        return retMap;
    }

    private PumpLocation buildTestPumpLocation() {
        return new PumpLocation.Builder().withPumpIn(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 1").build())
                .withPumpOut(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 2").build())
                .withPumpBelow(new CwmsId.Builder().withOfficeId(OFFICE).withName("Sacramento River Delta-Dam Water Pump 3").build()).build();
    }
}
