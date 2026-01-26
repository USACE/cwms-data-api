/*
 *
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda.data.dao.watersupply;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import mil.army.usace.hec.metadata.Parameter;
import mil.army.usace.hec.metadata.UnitUtil;
import org.junit.jupiter.api.Test;

class WaterSupplyUtilsTest {

    @Test
    void convertsCfsFlowsToSi() throws Exception {
        String office = "SPK";
        WaterUser user = new WaterUser.Builder()
                .withEntityName("Entity")
                .withProjectId(new CwmsId.Builder().withOfficeId(office).withName("Project").build())
                .withWaterRight("Right").build();
        PumpLocation locations = new PumpLocation.Builder()
                .withPumpIn(CwmsId.buildCwmsId(office, "Pump In"))
                .withPumpOut(CwmsId.buildCwmsId(office, "Pump Out"))
                .withPumpBelow(CwmsId.buildCwmsId(office, "Pump Below")).build();

        Map<Instant, List<PumpTransfer>> accountingMap = new TreeMap<>();
        List<PumpTransfer> list = new ArrayList<>();
        list.add(new PumpTransfer(PumpType.IN, "Pipeline", 1.0, "cfs", "Comment"));
        list.add(new PumpTransfer(PumpType.OUT, "Pipeline", 2.5, "cfs", "Comment"));
        accountingMap.put(Instant.parse("2025-10-01T00:00:00Z"), list);

        WaterSupplyAccounting accounting = new WaterSupplyAccounting.Builder()
                .withWaterUser(user)
                .withContractName("Contract")
                .withPumpLocations(locations)
                .withPumpAccounting(accountingMap)
                .build();

        //convert to SI
        WaterSupplyAccounting converted = WaterSupplyUtils.convertAccountingFlowsToSi(accounting);

        //Units and values converted
        String siUnits = Parameter.getParameter(Parameter.PARAMID_FLOW).getUnitsStringForSystem(UnitUtil.SI_ID);
        assertNotNull(converted);
        assertEquals(accounting.getContractName(), converted.getContractName());
        assertEquals(accounting.getWaterUser(), converted.getWaterUser());
        assertEquals(accounting.getPumpLocations(), converted.getPumpLocations());

        for (Map.Entry<Instant, List<PumpTransfer>> e : converted.getPumpAccounting().entrySet()) {
            for (PumpTransfer pt : e.getValue()) {
                assertEquals(siUnits, pt.getFlowUnit());
            }
        }

        List<PumpTransfer> convertedList = converted.getPumpAccounting().get(Instant.parse("2025-10-01T00:00:00Z"));
        double expected1 = UnitUtil.convertUnits(1.0, "cfs", siUnits);
        double expected2 = UnitUtil.convertUnits(2.5, "cfs", siUnits);
        assertEquals(expected1, convertedList.get(0).getFlow(), 1e-6);
        assertEquals(expected2, convertedList.get(1).getFlow(), 1e-6);
    }

    @Test
    void convertCmsFlowsToSiDoesNothing() throws Exception {
        String office = "SPK";
        WaterUser user = new WaterUser.Builder()
                .withEntityName("Entity")
                .withProjectId(new CwmsId.Builder().withOfficeId(office).withName("Project").build())
                .withWaterRight("Right").build();
        PumpLocation locations = new PumpLocation.Builder()
                .withPumpIn(CwmsId.buildCwmsId(office, "Pump In"))
                .withPumpOut(CwmsId.buildCwmsId(office, "Pump Out"))
                .withPumpBelow(CwmsId.buildCwmsId(office, "Pump Below")).build();

        Map<Instant, List<PumpTransfer>> accountingMap = new TreeMap<>();
        List<PumpTransfer> list = new ArrayList<>();
        list.add(new PumpTransfer(PumpType.IN, "Pipeline", 1.0, "cms", "Comment"));
        list.add(new PumpTransfer(PumpType.OUT, "Pipeline", 2.5, "cms", "Comment"));
        accountingMap.put(Instant.parse("2025-10-01T00:00:00Z"), list);

        WaterSupplyAccounting accounting = new WaterSupplyAccounting.Builder()
                .withWaterUser(user)
                .withContractName("Contract")
                .withPumpLocations(locations)
                .withPumpAccounting(accountingMap)
                .build();

        //convert to SI
        WaterSupplyAccounting converted = WaterSupplyUtils.convertAccountingFlowsToSi(accounting);

        //Units and values unchanged
        assertNotNull(converted);
        assertEquals(accounting.getContractName(), converted.getContractName());
        assertEquals(accounting.getWaterUser(), converted.getWaterUser());
        assertEquals(accounting.getPumpLocations(), converted.getPumpLocations());

        for (Map.Entry<Instant, List<PumpTransfer>> e : converted.getPumpAccounting().entrySet()) {
            for (PumpTransfer pt : e.getValue()) {
                assertEquals("cms", pt.getFlowUnit());
            }
        }

        List<PumpTransfer> convertedList = converted.getPumpAccounting().get(Instant.parse("2025-10-01T00:00:00Z"));
        assertEquals(1.0, convertedList.get(0).getFlow(), 1e-6);
        assertEquals(2.5, convertedList.get(1).getFlow(), 1e-6);
    }
}
