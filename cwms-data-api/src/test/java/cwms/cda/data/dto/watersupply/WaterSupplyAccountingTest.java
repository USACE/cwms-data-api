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
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


class WaterSupplyAccountingTest {
    private static final String OFFICE = "SPK";

    @Test
    void testWaterSupplyAccountingSerializationRoundTrip() {
        WaterUser user = new WaterUser.Builder().withEntityName("Test Entity")
            .withProjectId(new CwmsId.Builder()
                .withOfficeId(OFFICE)
                .withName("Test Location")
                .build())
            .withWaterRight("Test Water Right").build();
        WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                .withWaterUser(user).withContractName("Test Contract").withPumpAccounting(buildPumpAccounting()).build();
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
                .withEntityName("Test Entity").withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                        .withName("Test Location").build())
                .withWaterRight("Test Water Right").build())
                .withContractName("Test Contract")
                .withPumpAccounting(buildPumpAccounting()).build();
        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/data/dto/watersupply/pump_accounting.json");
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
                        .withWaterUser(new WaterUser.Builder().withEntityName("Test Entity")
                                .withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                                .withName("Test Location").build())
                        .withWaterRight("Test Water Right").build()).withContractName("Test Contract")
                        .build();
                assertDoesNotThrow(waterSupplyAccounting::validate, "Expected validation to pass");
            },
            () -> {
                WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                        .withWaterUser(new WaterUser.Builder()
                        .withEntityName("Test Entity").withProjectId(new CwmsId.Builder().withOfficeId(OFFICE)
                                .withName("Test Location").build())
                        .withWaterRight("Test Water Right").build())
                        .withContractName(null)
                        .build();
                assertThrows(FieldException.class, waterSupplyAccounting::validate, "Expected validation to "
                    + "fail due to null contract name");
            },
            () -> {
                WaterSupplyAccounting waterSupplyAccounting = new WaterSupplyAccounting.Builder()
                        .withContractName("Test Contract")
                        .build();
                assertThrows(FieldException.class, waterSupplyAccounting::validate, "Expected validation to "
                    + "fail due to null water user");
            }
        );
    }


    private static List<PumpAccounting> buildPumpAccounting() {
        List<PumpAccounting> pumpList = new ArrayList<>();
        PumpAccounting pumpAccounting = new PumpAccounting.Builder()
                .withPumpLocation(new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location-Test Pump").build())
                .withTransferType(new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip").withOfficeId(OFFICE)
                .withDisplayValue("Test Transfer Type").build()).withFlow(1.0)
                .withTransferDate(Instant.ofEpochMilli(10000012648000L)).withComment("Test Comment").build();
        pumpList.add(pumpAccounting);
        PumpAccounting pumpAccounting2 = new PumpAccounting.Builder()
                .withPumpLocation(new CwmsId.Builder().withOfficeId(OFFICE).withName("Test Location-Test Pump 2").build())
                .withTransferType(new LookupType.Builder().withActive(true).withTooltip("Test Tool Tip 2").withOfficeId(OFFICE)
                .withDisplayValue("Test Transfer Type 2").build()).withFlow(2.0)
                .withTransferDate(Instant.ofEpochMilli(10000012648000L)).withComment("Test Comment 2").build();
        pumpList.add(pumpAccounting2);
        return pumpList;
    }
}
