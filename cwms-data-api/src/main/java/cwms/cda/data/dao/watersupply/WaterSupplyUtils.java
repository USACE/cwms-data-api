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

package cwms.cda.data.dao.watersupply;

import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_TAB_T;


final class WaterSupplyUtils {

    private WaterSupplyUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static WaterUserContract toWaterContract(WATER_USER_CONTRACT_OBJ_T contract) {
        return new WaterUserContract.Builder().withContractedStorage(contract.getCONTRACTED_STORAGE())
                .withTotalAllocPercentActivated(contract.getTOTAL_ALLOC_PERCENT_ACTIVATED())
                .withContractType(LocationUtil.getLookupType(contract.getWATER_SUPPLY_CONTRACT_TYPE()))
                .withContractEffectiveDate(contract.getWS_CONTRACT_EFFECTIVE_DATE().toInstant())
                .withOfficeId(contract.getWATER_SUPPLY_CONTRACT_TYPE().getOFFICE_ID())
                .withStorageUnitsId(contract.getSTORAGE_UNITS_ID())
                .withContractExpirationDate(contract.getWS_CONTRACT_EXPIRATION_DATE().toInstant())
                .withWaterUser(toWaterUser(contract.getWATER_USER_CONTRACT_REF().getWATER_USER()))
                .withContractId(new CwmsId.Builder().withOfficeId(contract.getWATER_SUPPLY_CONTRACT_TYPE()
                                .getOFFICE_ID()).withName(contract.getWATER_USER_CONTRACT_REF()
                        .getCONTRACT_NAME()).build())
                .withFutureUseAllocation(contract.getFUTURE_USE_ALLOCATION())
                .withFutureUsePercentActivated(contract.getFUTURE_USE_PERCENT_ACTIVATED())
                .withInitialUseAllocation(contract.getINITIAL_USE_ALLOCATION())
                .withPumpOutLocation(contract.getPUMP_OUT_LOCATION() != null
                        ? new WaterSupplyPump.Builder().withPumpLocation(LocationUtil
                        .getLocation(contract.getPUMP_OUT_LOCATION())).withPumpType(PumpType.OUT).build() : null)
                .withPumpOutBelowLocation(contract.getPUMP_OUT_BELOW_LOCATION() != null
                        ? new WaterSupplyPump.Builder().withPumpLocation(LocationUtil
                        .getLocation(contract.getPUMP_OUT_BELOW_LOCATION()))
                        .withPumpType(PumpType.BELOW).build() : null)
                .withPumpInLocation(contract.getPUMP_IN_LOCATION() != null
                        ? new WaterSupplyPump.Builder().withPumpLocation(LocationUtil
                        .getLocation(contract.getPUMP_IN_LOCATION())).withPumpType(PumpType.IN).build() : null)
                .build();
    }

    public static WaterUser toWaterUser(WATER_USER_OBJ_T waterUserTabT) {
        return new WaterUser.Builder().withEntityName(waterUserTabT.getENTITY_NAME())
                .withProjectId(new CwmsId.Builder().withName(waterUserTabT.getPROJECT_LOCATION_REF()
                        .call_GET_LOCATION_ID()).withOfficeId(waterUserTabT.getPROJECT_LOCATION_REF()
                        .getOFFICE_ID()).build())
                .withWaterRight(waterUserTabT.getWATER_RIGHT()).build();
    }

    public static WATER_USER_OBJ_T toWaterUser(WaterUser waterUser) {
        WATER_USER_OBJ_T waterUserObjT = new WATER_USER_OBJ_T();
        waterUserObjT.setENTITY_NAME(waterUser.getEntityName());
        waterUserObjT.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(waterUser.getProjectId()));
        waterUserObjT.setWATER_RIGHT(waterUser.getWaterRight());
        return waterUserObjT;
    }

    public static WATER_USER_TAB_T toWaterUserTs(WaterUser waterUser) {
        WATER_USER_OBJ_T waterUserObjT = new WATER_USER_OBJ_T();
        waterUserObjT.setENTITY_NAME(waterUser.getEntityName());
        waterUserObjT.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(waterUser.getProjectId()));
        waterUserObjT.setWATER_RIGHT(waterUser.getWaterRight());
        List<WATER_USER_OBJ_T> waterUserList = new ArrayList<>();
        waterUserList.add(waterUserObjT);
        return new WATER_USER_TAB_T(waterUserList);
    }

    public static LOOKUP_TYPE_OBJ_T toLookupTypeT(LookupType lookupType) {
        LOOKUP_TYPE_OBJ_T lookupTypeObjT = new LOOKUP_TYPE_OBJ_T();
        lookupTypeObjT.setOFFICE_ID(lookupType.getOfficeId());
        lookupTypeObjT.setDISPLAY_VALUE(lookupType.getDisplayValue());
        lookupTypeObjT.setTOOLTIP(lookupType.getTooltip());
        lookupTypeObjT.setACTIVE(OracleTypeMap.formatBool(lookupType.getActive()));
        return lookupTypeObjT;
    }

    public static LOOKUP_TYPE_TAB_T toLookupTypeT(List<LookupType> lookupTypes) {
        List<LOOKUP_TYPE_OBJ_T> lookupTypeList = new ArrayList<>();
        for (LookupType lookupType : lookupTypes) {
            lookupTypeList.add(toLookupTypeT(lookupType));
        }
        return new LOOKUP_TYPE_TAB_T(lookupTypeList);
    }

    public static WATER_USER_CONTRACT_REF_T toContractRef(WaterUser waterUser, String contractName) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUser(waterUser));
        waterUserContractRefT.setCONTRACT_NAME(contractName);
        return waterUserContractRefT;
    }

    public static WATER_USER_CONTRACT_REF_T toWaterUserContractRefTs(WaterUserContract waterUserContract) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUser(waterUserContract.getWaterUser()));
        waterUserContractRefT.setCONTRACT_NAME(waterUserContract.getContractId().getName());
        return waterUserContractRefT;
    }

    public static WATER_USER_CONTRACT_TAB_T toWaterUserContractTs(WaterUserContract waterUserContract) {
        WATER_USER_CONTRACT_OBJ_T waterUserContractObjT = new WATER_USER_CONTRACT_OBJ_T();
        waterUserContractObjT.setCONTRACTED_STORAGE(waterUserContract.getContractedStorage());
        waterUserContractObjT.setTOTAL_ALLOC_PERCENT_ACTIVATED(waterUserContract.getTotalAllocPercentActivated());
        waterUserContractObjT.setWS_CONTRACT_EFFECTIVE_DATE(new Timestamp(waterUserContract
                .getContractEffectiveDate().toEpochMilli()));
        waterUserContractObjT.setSTORAGE_UNITS_ID(waterUserContract.getStorageUnitsId());
        waterUserContractObjT.setWS_CONTRACT_EXPIRATION_DATE(new Timestamp(waterUserContract
                .getContractExpirationDate().toEpochMilli()));
        waterUserContractObjT.setWATER_USER_CONTRACT_REF(toContractRef(waterUserContract.getWaterUser(),
                waterUserContract.getContractId().getName()));
        waterUserContractObjT.setWATER_SUPPLY_CONTRACT_TYPE(toLookupTypeT(waterUserContract.getContractType()));
        waterUserContractObjT.setFUTURE_USE_ALLOCATION(waterUserContract.getFutureUseAllocation());
        waterUserContractObjT.setFUTURE_USE_PERCENT_ACTIVATED(waterUserContract.getFutureUsePercentActivated());
        waterUserContractObjT.setINITIAL_USE_ALLOCATION(waterUserContract.getInitialUseAllocation());
        waterUserContractObjT.setPUMP_OUT_LOCATION(waterUserContract.getPumpOutLocation() != null
                ? LocationUtil.getLocation(waterUserContract.getPumpOutLocation().getPumpLocation()) : null);
        waterUserContractObjT.setPUMP_OUT_BELOW_LOCATION(waterUserContract.getPumpOutBelowLocation() != null
                ? LocationUtil.getLocation(waterUserContract.getPumpOutBelowLocation().getPumpLocation()) : null);
        waterUserContractObjT.setPUMP_IN_LOCATION(waterUserContract.getPumpInLocation() != null
                ? LocationUtil.getLocation(waterUserContract.getPumpInLocation().getPumpLocation()) : null);

        List<WATER_USER_CONTRACT_OBJ_T> contractList = new ArrayList<>();
        contractList.add(waterUserContractObjT);
        return new WATER_USER_CONTRACT_TAB_T(contractList);
    }
}
