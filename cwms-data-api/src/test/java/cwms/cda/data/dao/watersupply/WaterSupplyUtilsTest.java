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
import static org.junit.jupiter.api.Assertions.assertNull;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import mil.army.usace.hec.metadata.Parameter;
import mil.army.usace.hec.metadata.UnitUtil;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;

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

    @Test
    void toWaterContractHandlesNullDates() {
        WATER_USER_CONTRACT_OBJ_T contract =
              createWaterUserContract("Project", "SPK", "Contract", "CWMS");

        contract.setWS_CONTRACT_EFFECTIVE_DATE(null);
        contract.setWS_CONTRACT_EXPIRATION_DATE(null);

        WaterUserContract result = WaterSupplyUtils.toWaterContract(contract);

        assertNull(result.getContractEffectiveDate());
        assertNull(result.getContractExpirationDate());
    }

    @Test
    void toWaterContractUsesProjectOfficeId() {
        String projectOffice = "SPK";
        String contractTypeOffice = "CWMS";

        WATER_USER_CONTRACT_OBJ_T contract =
              createWaterUserContract("Project", projectOffice, "Contract", contractTypeOffice);

        WaterUserContract result = WaterSupplyUtils.toWaterContract(contract);

        assertNotNull(result);
        assertEquals(projectOffice, result.getOfficeId());
    }

    @Test
    void toWaterContractUsesProjectOfficeIdForContractId() {
        String projectOffice = "SPK";
        String contractTypeOffice = "CWMS";

        WATER_USER_CONTRACT_OBJ_T contract =
              createWaterUserContract("Project", projectOffice, "Contract", contractTypeOffice);

        WaterUserContract result = WaterSupplyUtils.toWaterContract(contract);

        assertNotNull(result.getContractId());
        assertEquals("Contract", result.getContractId().getName());
        assertEquals(projectOffice, result.getContractId().getOfficeId());
    }

    private static WATER_USER_CONTRACT_OBJ_T createWaterUserContract(
          String projectId,
          String projectOffice,
          String contractName,
          String contractTypeOffice) {

        LOCATION_REF_T projectLocationRef = new LOCATION_REF_T();
        projectLocationRef.setBASE_LOCATION_ID(projectId);
        projectLocationRef.setOFFICE_ID(projectOffice);

        WATER_USER_OBJ_T waterUser = new WATER_USER_OBJ_T();
        waterUser.setPROJECT_LOCATION_REF(projectLocationRef);
        waterUser.setENTITY_NAME("Entity");
        waterUser.setWATER_RIGHT("Right");

        WATER_USER_CONTRACT_REF_T contractRef = new WATER_USER_CONTRACT_REF_T();
        contractRef.setWATER_USER(waterUser);
        contractRef.setCONTRACT_NAME(contractName);

        LOOKUP_TYPE_OBJ_T contractType = new LOOKUP_TYPE_OBJ_T();
        contractType.setOFFICE_ID(contractTypeOffice);
        contractType.setDISPLAY_VALUE("Storage");
        contractType.setACTIVE("T");

        WATER_USER_CONTRACT_OBJ_T contract = new WATER_USER_CONTRACT_OBJ_T();
        contract.setWATER_USER_CONTRACT_REF(contractRef);
        contract.setWATER_SUPPLY_CONTRACT_TYPE(contractType);

        return contract;
    }
}
