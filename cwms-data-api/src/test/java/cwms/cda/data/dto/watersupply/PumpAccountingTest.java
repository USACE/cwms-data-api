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
import java.util.Map;
import java.util.TreeMap;


class PumpAccountingTest {
    private static final String OFFICE = "SPK";

    @Test
    void testWaterSupplyPumpAccountingSerializationRoundTrip() {
        WaterSupplyAccounting pumpAccounting = buildTestAccounting();
        String serialized = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterSupplyAccounting.class),
                pumpAccounting);
        WaterSupplyAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
            WaterSupplyAccounting.class), serialized, WaterSupplyAccounting.class);
        DTOMatch.assertMatch(pumpAccounting, deserialized);
    }

    @Test
    void testWaterSupplyPumpAccountingSerializationRoundTripFromFile() throws Exception {
        WaterSupplyAccounting pumpAccounting = buildTestAccounting();
        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/data/dto/watersupply/water_supply_accounting.json");
        assertNotNull(resource);
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        WaterSupplyAccounting deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                WaterSupplyAccounting.class), serialized, WaterSupplyAccounting.class);
        DTOMatch.assertMatch(pumpAccounting, deserialized);
    }

    @Test
    void testValidate() {
        assertAll(
            () -> {
                Map<Instant, PumpTransfer> pumpMap = new TreeMap<>();
                pumpMap.put(Instant.ofEpochSecond(10000012648112L), new PumpTransfer.Builder()
                        .withTransferDate(Instant.ofEpochSecond(10000012648112L))
                        .withTransferTypeDisplay("Test Transfer Type").withFlow(1.0)
                        .withComment("Test Comment").build());
                PumpAccounting pumpAccounting = new PumpAccounting.Builder()
                        .withPumpLocation(new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Pump").build())
                        .withPumpTransfers(pumpMap).build();
                assertDoesNotThrow(pumpAccounting::validate, "Expected validation to pass");
            },
            () -> {
                Map<Instant, PumpTransfer> pumpMap = new TreeMap<>();
                pumpMap.put(Instant.ofEpochSecond(10000012648112L), new PumpTransfer.Builder()
                        .withTransferDate(Instant.ofEpochSecond(10000012648112L))
                        .withTransferTypeDisplay("Test Transfer Type").withFlow(1.0)
                        .withComment("Test Comment").build());
                PumpAccounting pumpAccounting = new PumpAccounting.Builder()
                        .withPumpLocation(null).withPumpTransfers(pumpMap).build();
                assertThrows(FieldException.class, pumpAccounting::validate, "Expected validation to "
                    + "fail due to null location");
            },
            () -> {
                PumpTransfer pumpTransfer = new PumpTransfer.Builder()
                        .withTransferDate(null)
                        .withTransferTypeDisplay("Test Transfer Type").withFlow(1.0)
                        .withComment("Test Comment").build();
                assertThrows(FieldException.class, pumpTransfer::validate, "Expected validation to "
                    + "fail due to null transfer date");
            },
            () -> {
                PumpTransfer pumpTransfer = new PumpTransfer.Builder()
                        .withTransferDate(Instant.ofEpochSecond(10000012648112L))
                        .withTransferTypeDisplay("Test Transfer Type").withFlow(null)
                        .withComment("Test Comment").build();
                assertThrows(FieldException.class, pumpTransfer::validate, "Expected validation to "
                    + "fail due to null flow value");
            },
            () -> {
                PumpTransfer pumpTransfer = new PumpTransfer.Builder()
                        .withTransferDate(Instant.ofEpochSecond(10000012648112L))
                        .withTransferTypeDisplay(null).withFlow(1.0)
                        .withComment(null).build();
                assertThrows(FieldException.class, pumpTransfer::validate, "Expected validation to "
                    + "fail due to null transfer type display value");
            }
        );
    }

    private WaterSupplyAccounting buildTestAccounting() {
        return new WaterSupplyAccounting.Builder().withWaterUser(new WaterUser.Builder().withEntityName("Test Entity")
                .withWaterRight("Test Water Right").withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                        .withName("Test Location").build()).build())
                .withContractName("Test Contract").withPumpInAccounting(buildTestPumpInAccountingList())
                .withPumpBelowAccounting(buildTestPumpInAccountingList())
                .build();
    }

    private Map<String, PumpAccounting> buildTestPumpInAccountingList() {
        Map<String, PumpAccounting> retMap = new TreeMap<>();
        Map<Instant, PumpTransfer> pumpMap = new TreeMap<>();
        pumpMap.put(Instant.ofEpochMilli(10000012648000L), new PumpTransfer.Builder().withTransferTypeDisplay("Test Transfer Type")
                .withFlow(1.0).withTransferDate(Instant.ofEpochMilli(10000012648000L)).withComment("Test Comment").build());
        pumpMap.put(Instant.ofEpochMilli(10000012649000L), new PumpTransfer.Builder().withTransferTypeDisplay("Test Transfer Type")
                .withFlow(2.0).withTransferDate(Instant.ofEpochMilli(10000012649000L)).withComment("Test Comment 2").build());
        PumpAccounting pumpAccounting = new PumpAccounting.Builder().withPumpLocation(new CwmsId.Builder()
                .withOfficeId(OFFICE).withName("Test Location-Test Pump").build()).withPumpTransfers(pumpMap).build();
        retMap.put("Test Pump", pumpAccounting);
        pumpMap = new TreeMap<>();
        pumpMap.put(Instant.ofEpochMilli(10000012699000L), new PumpTransfer.Builder().withTransferTypeDisplay("Test Transfer Type2")
                .withFlow(1.0).withTransferDate(Instant.ofEpochMilli(10000012699000L)).withComment("Test Comment").build());
        pumpMap.put(Instant.ofEpochMilli(10000012710000L), new PumpTransfer.Builder().withTransferTypeDisplay("Test Transfer Type2")
                .withFlow(2.0).withTransferDate(Instant.ofEpochMilli(10000012710000L)).withComment("Test Comment 2").build());
        pumpAccounting = new PumpAccounting.Builder().withPumpLocation(new CwmsId.Builder()
                .withOfficeId(OFFICE).withName("Test Location-Test Pump2").build()).withPumpTransfers(pumpMap).build();
        retMap.put("Test Pump2", pumpAccounting);
        return retMap;
    }
}
