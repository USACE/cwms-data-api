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

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.Dao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen_shadow.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.LOC_REF_TIME_WINDOW_OBJ_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.WAT_USR_CONTRACT_ACCT_OBJ_T;
import usace.cwms.db.jooq.codegen_shadow.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;

/**
 * This class is a stop-gap solution paired with the shaded jOOQ codegen jar for older schema versions
 * that do not support water supply flow units (i.e., 25.07.01 and earlier).
 */
@Deprecated(forRemoval = true)
final class WaterSupplyUtilsShadow {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private WaterSupplyUtilsShadow() {
        throw new AssertionError("Utility class");
    }

    private static WaterUser toWaterUser(WATER_USER_OBJ_T waterUserTabT) {
        return new WaterUser.Builder().withEntityName(waterUserTabT.getENTITY_NAME())
            .withProjectId(new CwmsId.Builder().withName(waterUserTabT.getPROJECT_LOCATION_REF()
                .call_GET_LOCATION_ID()).withOfficeId(waterUserTabT.getPROJECT_LOCATION_REF()
                .getOFFICE_ID()).build())
            .withWaterRight(waterUserTabT.getWATER_RIGHT()).build();
    }

    private static WATER_USER_OBJ_T toWaterUserObjT(WaterUser waterUser) {
        var waterUserObjT = new WATER_USER_OBJ_T();
        waterUserObjT.setENTITY_NAME(waterUser.getEntityName());
        waterUserObjT.setPROJECT_LOCATION_REF(getLocationRef(waterUser.getProjectId()));
        waterUserObjT.setWATER_RIGHT(waterUser.getWaterRight());
        return waterUserObjT;
    }

    private static LOCATION_REF_T getLocationRef(CwmsId cwmsId) {
        LOCATION_REF_T retval = null;
        if (cwmsId != null) {
            retval = new LOCATION_REF_T();
            String[] split = cwmsId.getName().split("-");
            retval.setBASE_LOCATION_ID(split[0]);
            if (split.length > 1) {
                retval.setSUB_LOCATION_ID(split[1]);
            }
            retval.setOFFICE_ID(cwmsId.getOfficeId());
        }
        return retval;
    }

    private static LOOKUP_TYPE_OBJ_T toLookupType(LookupType lookupType) {
        var lookupTypeObjT = new LOOKUP_TYPE_OBJ_T();
        lookupTypeObjT.setOFFICE_ID(lookupType.getOfficeId());
        lookupTypeObjT.setDISPLAY_VALUE(lookupType.getDisplayValue());
        lookupTypeObjT.setTOOLTIP(lookupType.getTooltip());
        lookupTypeObjT.setACTIVE(Dao.formatBool(lookupType.getActive()));
        return lookupTypeObjT;
    }

    static WATER_USER_CONTRACT_REF_T toContractRef(WaterUser waterUser, String contractName) {
        var waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUserObjT(waterUser));
        waterUserContractRefT.setCONTRACT_NAME(contractName);
        return waterUserContractRefT;
    }

    private static LOCATION_REF_T getPumpLocationRef(PumpType pumpType,
        LOCATION_REF_T pumpIn,
        LOCATION_REF_T pumpOut,
        LOCATION_REF_T pumpBelow) {
        switch (pumpType) {
            case IN:
                return pumpIn;
            case OUT:
                return pumpOut;
            case BELOW:
                return pumpBelow;
            default:
                logger.atWarning().log("Invalid pump type");
                throw new IllegalArgumentException(
                    String.format("Invalid pump type for mapping to DB object: %s", pumpType));
        }
    }

    static LOC_REF_TIME_WINDOW_TAB_T toTimeWindowTabT(WaterSupplyAccounting accounting) {
        List<LOC_REF_TIME_WINDOW_OBJ_T> timeWindowList = new ArrayList<>();
        var pumpIn = getLocationRef(accounting.getPumpLocations().getPumpIn());
        var pumpOut = getLocationRef(accounting.getPumpLocations().getPumpOut());
        var pumpBelow = getLocationRef(accounting.getPumpLocations().getPumpBelow());

        for (Map.Entry<Instant, List<PumpTransfer>> entry : accounting.getPumpAccounting().entrySet()) {
            for (PumpTransfer transfer : entry.getValue()) {
                var timeWindow = new LOC_REF_TIME_WINDOW_OBJ_T();
                switch (transfer.getPumpType()) {
                    case IN:
                        timeWindow.setLOCATION_REF(pumpIn);
                        break;
                    case OUT:
                        timeWindow.setLOCATION_REF(pumpOut);
                        break;
                    case BELOW:
                        timeWindow.setLOCATION_REF(pumpBelow);
                        break;
                    default:
                        logger.atWarning().log("Invalid pump type");
                        break;
                }
                timeWindow.setSTART_DATE(Timestamp.from(entry.getKey()));
                timeWindow.setEND_DATE(Timestamp.from(entry.getKey()));
                timeWindowList.add(timeWindow);
            }
        }
        return new LOC_REF_TIME_WINDOW_TAB_T(timeWindowList);
    }

    static List<WaterSupplyAccounting> toWaterSupplyAccountingList(Connection c,
        WAT_USR_CONTRACT_ACCT_TAB_T watUsrContractAcctTabT, String flowUnits) {

        List<WaterSupplyAccounting> waterSupplyAccounting = new ArrayList<>();
        Map<WaterSupplyUtils.AccountingKey, WaterSupplyAccounting> cacheMap = new TreeMap<>();

        for (WAT_USR_CONTRACT_ACCT_OBJ_T watUsrContractAcctObjT : watUsrContractAcctTabT) {
            WATER_USER_CONTRACT_REF_T watUsrContractRef =
                watUsrContractAcctObjT.getWATER_USER_CONTRACT_REF();
            WaterSupplyUtils.AccountingKey key = new WaterSupplyUtils.AccountingKey.Builder()
                .withContractName(watUsrContractRef.getCONTRACT_NAME())
                .withWaterUser(new WaterUser.Builder()
                    .withWaterRight(watUsrContractRef.getWATER_USER().getWATER_RIGHT())
                    .withEntityName(watUsrContractRef.getWATER_USER().getENTITY_NAME())
                    .withProjectId(
                        CwmsId.buildCwmsId(watUsrContractRef.getWATER_USER().getPROJECT_LOCATION_REF().getOFFICE_ID(),
                            watUsrContractRef.getWATER_USER().getPROJECT_LOCATION_REF().call_GET_LOCATION_ID()))
                    .build())
                .build();
            if (cacheMap.containsKey(key)) {
                WaterSupplyAccounting accounting = cacheMap.get(key);
                addTransfer(watUsrContractAcctObjT, accounting, flowUnits);
            } else {
                cacheMap.put(key, createAccounting(c, watUsrContractAcctObjT, flowUnits));
            }
        }
        for (Map.Entry<WaterSupplyUtils.AccountingKey, WaterSupplyAccounting> entry : cacheMap.entrySet()) {
            waterSupplyAccounting.add(entry.getValue());
        }
        return waterSupplyAccounting;
    }

    private static WaterSupplyAccounting createAccounting(Connection c,
        WAT_USR_CONTRACT_ACCT_OBJ_T acctObjT, String flowUnits) {
        WaterContractDao waterContractDao = new WaterContractDao(DSL.using(c));
        WATER_USER_OBJ_T waterUserObjT =
            acctObjT.getWATER_USER_CONTRACT_REF().getWATER_USER();
        WaterUserContract waterUserContract = waterContractDao.getWaterContract(
            acctObjT.getWATER_USER_CONTRACT_REF().getCONTRACT_NAME(),
            new CwmsId.Builder()
                .withOfficeId(waterUserObjT.getPROJECT_LOCATION_REF().getOFFICE_ID())
                .withName(waterUserObjT.getPROJECT_LOCATION_REF().call_GET_LOCATION_ID())
                .build(),
            waterUserObjT.getENTITY_NAME());
        Map<Instant, List<PumpTransfer>> pumpAccounting = new TreeMap<>();
        String pumpLocation = acctObjT.getPUMP_LOCATION_REF().call_GET_LOCATION_ID();
        String pumpOffice = acctObjT.getPUMP_LOCATION_REF().getOFFICE_ID();
        String transferDisplay = acctObjT.getPHYSICAL_TRANSFER_TYPE().getDISPLAY_VALUE();
        Location pumpIn = waterUserContract.getPumpInLocation() != null
            ? waterUserContract.getPumpInLocation().getPumpLocation() : null;
        Location pumpOut = waterUserContract.getPumpOutLocation() != null
            ? waterUserContract.getPumpOutLocation().getPumpLocation() : null;
        Location pumpBelow = waterUserContract.getPumpOutBelowLocation() != null
            ? waterUserContract.getPumpOutBelowLocation().getPumpLocation() : null;
        Instant transferStart = acctObjT.getTRANSFER_START_DATETIME().toInstant();
        String remarks = acctObjT.getACCOUNTING_REMARKS();
        double flow = acctObjT.getPUMP_FLOW();

        PumpTransfer transfer = null;
        if (pumpIn != null && pumpIn.getName().equalsIgnoreCase(pumpLocation)
            && pumpIn.getOfficeId().equalsIgnoreCase(pumpOffice)) {
            transfer = new PumpTransfer(PumpType.IN, transferDisplay, flow, flowUnits, remarks);
        } else if (pumpOut != null && pumpOut.getName().equalsIgnoreCase(pumpLocation)
            && pumpOut.getOfficeId().equalsIgnoreCase(pumpOffice)) {
            transfer = new PumpTransfer(PumpType.OUT, transferDisplay, flow, flowUnits, remarks);
        } else if (pumpBelow != null && pumpBelow.getName().equalsIgnoreCase(pumpLocation)
            && pumpBelow.getOfficeId().equalsIgnoreCase(pumpOffice)) {
            transfer = new PumpTransfer(PumpType.BELOW, transferDisplay, flow, flowUnits, remarks);
        }
        if (transfer != null) {
            pumpAccounting.put(transferStart, Collections.singletonList(transfer));
        }
        return new WaterSupplyAccounting.Builder()
            .withContractName(acctObjT.getWATER_USER_CONTRACT_REF().getCONTRACT_NAME())
            .withWaterUser(toWaterUser(waterUserObjT))
            .withPumpLocations(new PumpLocation.Builder()
                .withPumpIn(pumpIn != null ? CwmsId.buildCwmsId(pumpIn.getOfficeId(), pumpIn.getName()) : null)
                .withPumpOut(pumpOut != null ? CwmsId.buildCwmsId(pumpOut.getOfficeId(), pumpOut.getName()) : null)
                .withPumpBelow(
                    pumpBelow != null ? CwmsId.buildCwmsId(pumpBelow.getOfficeId(), pumpBelow.getName()) : null)
                .build())
            .withPumpAccounting(pumpAccounting)
            .build();
    }

    private static void addTransfer(WAT_USR_CONTRACT_ACCT_OBJ_T acctObjTs, WaterSupplyAccounting accounting,
        String flowUnits) {
        PumpTransfer transfer = null;
        String transferDisplay = acctObjTs.getPHYSICAL_TRANSFER_TYPE().getDISPLAY_VALUE();
        String accountingRemarks = acctObjTs.getACCOUNTING_REMARKS();
        Instant transferStart = acctObjTs.getTRANSFER_START_DATETIME().toInstant();
        String officeId = acctObjTs.getPUMP_LOCATION_REF().getOFFICE_ID();
        String locationId = acctObjTs.getPUMP_LOCATION_REF().call_GET_LOCATION_ID();
        CwmsId pumpIn = accounting.getPumpLocations().getPumpIn();
        CwmsId pumpOut = accounting.getPumpLocations().getPumpOut();
        CwmsId pumpBelow = accounting.getPumpLocations().getPumpBelow();

        if (pumpIn != null && pumpIn.getName().equalsIgnoreCase(locationId)
            && pumpIn.getOfficeId().equalsIgnoreCase(officeId)) {
            transfer =
                new PumpTransfer(PumpType.IN, transferDisplay, acctObjTs.getPUMP_FLOW(), flowUnits, accountingRemarks);
        } else if (pumpOut != null && pumpOut.getName().equalsIgnoreCase(locationId)
            && pumpOut.getOfficeId().equalsIgnoreCase(officeId)) {
            transfer =
                new PumpTransfer(PumpType.OUT, transferDisplay, acctObjTs.getPUMP_FLOW(), flowUnits, accountingRemarks);
        } else if (pumpBelow != null && pumpBelow.getName().equalsIgnoreCase(locationId)
            && pumpBelow.getOfficeId().equalsIgnoreCase(officeId)) {
            transfer = new PumpTransfer(PumpType.BELOW, transferDisplay,
                acctObjTs.getPUMP_FLOW(), flowUnits, accountingRemarks);
        }
        if (accounting.getPumpAccounting().get(transferStart) != null) {
            List<PumpTransfer> transfers = new ArrayList<>(accounting.getPumpAccounting().get(transferStart));
            transfers.add(transfer);
            accounting.getPumpAccounting().put(transferStart, transfers);
            return;
        }
        accounting.getPumpAccounting().put(transferStart,
            Collections.singletonList(transfer));
    }

    static WAT_USR_CONTRACT_ACCT_TAB_T toManualWaterUserContractAcctTs(WaterSupplyAccounting accounting) {
        List<WAT_USR_CONTRACT_ACCT_OBJ_T> watUsrContractAcctObjTList =
            new ArrayList<>();
        var pumpIn = getLocationRef(accounting.getPumpLocations().getPumpIn());
        var pumpOut = getLocationRef(accounting.getPumpLocations().getPumpOut());
        var pumpBelow = getLocationRef(accounting.getPumpLocations().getPumpBelow());

        for (var entry : accounting.getPumpAccounting().entrySet()) {
            for (var transfer : entry.getValue()) {
                var watUsrContractAcctObjT = getManualWatUsrContractAcctObjT(accounting, entry, transfer, pumpIn,
                    pumpOut, pumpBelow);
                watUsrContractAcctObjTList.add(watUsrContractAcctObjT);
            }
        }
        return new WAT_USR_CONTRACT_ACCT_TAB_T(
            watUsrContractAcctObjTList);
    }

    private static WAT_USR_CONTRACT_ACCT_OBJ_T getManualWatUsrContractAcctObjT(
        WaterSupplyAccounting accounting,
        Map.Entry<Instant, List<PumpTransfer>> entry,
        PumpTransfer transfer,
        LOCATION_REF_T pumpIn,
        LOCATION_REF_T pumpOut,
        LOCATION_REF_T pumpBelow) {
        var watUsrContractAcctObjT = new WAT_USR_CONTRACT_ACCT_OBJ_T();
        var contractRef = toContractRef(accounting.getWaterUser(), accounting.getContractName());
        watUsrContractAcctObjT.setWATER_USER_CONTRACT_REF(contractRef);
        watUsrContractAcctObjT.setACCOUNTING_REMARKS(transfer.getComment());
        watUsrContractAcctObjT.setPUMP_FLOW(transfer.getFlow());
        var transferType = toLookupType(new LookupType.Builder()
            .withDisplayValue(transfer.getTransferTypeDisplay())
            .withActive(true)
            .withOfficeId(accounting.getWaterUser().getProjectId().getOfficeId())
            .build());
        watUsrContractAcctObjT.setPHYSICAL_TRANSFER_TYPE(transferType);
        watUsrContractAcctObjT.setPUMP_LOCATION_REF(
            getPumpLocationRef(transfer.getPumpType(), pumpIn, pumpOut, pumpBelow));
        watUsrContractAcctObjT.setTRANSFER_START_DATETIME(Timestamp.from(entry.getKey()));
        return watUsrContractAcctObjT;
    }
}
