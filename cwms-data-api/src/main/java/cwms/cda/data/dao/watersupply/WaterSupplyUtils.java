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

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;


final class WaterSupplyUtils {
    private static final Logger LOGGER = Logger.getLogger(WaterSupplyUtils.class.getName());

    private WaterSupplyUtils() {
        throw new IllegalStateException("Utility class");
    }

    static WaterUserContract toWaterContract(WATER_USER_CONTRACT_OBJ_T contract) {
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

    static WaterUser toWaterUser(WATER_USER_OBJ_T waterUserTabT) {
        return new WaterUser.Builder().withEntityName(waterUserTabT.getENTITY_NAME())
                .withProjectId(new CwmsId.Builder().withName(waterUserTabT.getPROJECT_LOCATION_REF()
                        .call_GET_LOCATION_ID()).withOfficeId(waterUserTabT.getPROJECT_LOCATION_REF()
                        .getOFFICE_ID()).build())
                .withWaterRight(waterUserTabT.getWATER_RIGHT()).build();
    }

    static WATER_USER_OBJ_T toWaterUserObjT(WaterUser waterUser) {
        WATER_USER_OBJ_T waterUserObjT = new WATER_USER_OBJ_T();
        waterUserObjT.setENTITY_NAME(waterUser.getEntityName());
        waterUserObjT.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(waterUser.getProjectId()));
        waterUserObjT.setWATER_RIGHT(waterUser.getWaterRight());
        return waterUserObjT;
    }

    static LOOKUP_TYPE_OBJ_T toLookupTypeO(LookupType lookupType) {
        LOOKUP_TYPE_OBJ_T lookupTypeObjT = new LOOKUP_TYPE_OBJ_T();
        lookupTypeObjT.setOFFICE_ID(lookupType.getOfficeId());
        lookupTypeObjT.setDISPLAY_VALUE(lookupType.getDisplayValue());
        lookupTypeObjT.setTOOLTIP(lookupType.getTooltip());
        lookupTypeObjT.setACTIVE(JooqDao.formatBool(lookupType.getActive()));
        return lookupTypeObjT;
    }

    static LOOKUP_TYPE_TAB_T toLookupTypeT(LookupType lookupType) {
        List<LOOKUP_TYPE_OBJ_T> lookupTypeList = new ArrayList<>();
        lookupTypeList.add(toLookupTypeO(lookupType));
        return new LOOKUP_TYPE_TAB_T(lookupTypeList);
    }

    static WATER_USER_CONTRACT_REF_T toContractRef(WaterUser waterUser, String contractName) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUserObjT(waterUser));
        waterUserContractRefT.setCONTRACT_NAME(contractName);
        return waterUserContractRefT;
    }

    static WATER_USER_CONTRACT_REF_T toWaterUserContractRefTs(WaterUserContract waterUserContract) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUserObjT(waterUserContract.getWaterUser()));
        waterUserContractRefT.setCONTRACT_NAME(waterUserContract.getContractId().getName());
        return waterUserContractRefT;
    }

    static WATER_USER_CONTRACT_TAB_T toWaterUserContractTs(WaterUserContract waterUserContract) {
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
        waterUserContractObjT.setWATER_SUPPLY_CONTRACT_TYPE(toLookupTypeO(waterUserContract.getContractType()));
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

    static WAT_USR_CONTRACT_ACCT_TAB_T toWaterUserContractAcctTs(WaterSupplyAccounting accounting) {
        List<WAT_USR_CONTRACT_ACCT_OBJ_T> watUsrContractAcctObjTList = new ArrayList<>();
        LOCATION_REF_T pumpIn = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpIn());
        LOCATION_REF_T pumpOut = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpOut());
        LOCATION_REF_T pumpBelow = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpBelow());

        for (Map.Entry<Instant, List<PumpTransfer>> entry : accounting.getPumpAccounting().entrySet()) {
            for (PumpTransfer transfer : entry.getValue()) {
                WAT_USR_CONTRACT_ACCT_OBJ_T watUsrContractAcctObjT = new WAT_USR_CONTRACT_ACCT_OBJ_T();
                WATER_USER_CONTRACT_REF_T contractRef = toContractRef(accounting.getWaterUser(),
                        accounting.getContractName());
                watUsrContractAcctObjT.setWATER_USER_CONTRACT_REF(contractRef);
                watUsrContractAcctObjT.setACCOUNTING_REMARKS(transfer.getComment());
                watUsrContractAcctObjT.setPUMP_FLOW(transfer.getFlow());
                LOOKUP_TYPE_OBJ_T transferType = toLookupTypeO(new LookupType.Builder()
                        .withDisplayValue(transfer.getTransferTypeDisplay())
                        .withActive(true)
                        .withOfficeId(accounting.getWaterUser().getProjectId().getOfficeId())
                        .build());
                watUsrContractAcctObjT.setPHYSICAL_TRANSFER_TYPE(transferType);
                switch (transfer.getPumpType()) {
                    case IN:
                        watUsrContractAcctObjT.setPUMP_LOCATION_REF(pumpIn);
                        break;
                    case OUT:
                        watUsrContractAcctObjT.setPUMP_LOCATION_REF(pumpOut);
                        break;
                    case BELOW:
                        watUsrContractAcctObjT.setPUMP_LOCATION_REF(pumpBelow);
                        break;
                    default:
                        LOGGER.log(Level.WARNING, "Invalid pump type");
                        throw new IllegalArgumentException(
                            String.format("Invalid pump type for mapping to DB object: %s", transfer.getPumpType()));
                }
                watUsrContractAcctObjT.setTRANSFER_START_DATETIME(Timestamp.from(entry.getKey()));
                watUsrContractAcctObjTList.add(watUsrContractAcctObjT);
            }
        }
        return new WAT_USR_CONTRACT_ACCT_TAB_T(watUsrContractAcctObjTList);
    }

    static LOC_REF_TIME_WINDOW_TAB_T toTimeWindowTabT(WaterSupplyAccounting accounting) {
        List<LOC_REF_TIME_WINDOW_OBJ_T> timeWindowList = new ArrayList<>();
        LOCATION_REF_T pumpIn = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpIn());
        LOCATION_REF_T pumpOut = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpOut());
        LOCATION_REF_T pumpBelow = LocationUtil.getLocationRef(accounting.getPumpLocations().getPumpBelow());

        for (Map.Entry<Instant, List<PumpTransfer>> entry : accounting.getPumpAccounting().entrySet()) {
            for (PumpTransfer transfer : entry.getValue()) {
                LOC_REF_TIME_WINDOW_OBJ_T timeWindow = new LOC_REF_TIME_WINDOW_OBJ_T();
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
                        LOGGER.log(Level.WARNING, "Invalid pump type");
                        break;
                }
                timeWindow.setSTART_DATE(Timestamp.from(entry.getKey()));
                timeWindow.setEND_DATE(Timestamp.from(entry.getKey()));
                timeWindowList.add(timeWindow);
            }
        }
        return new LOC_REF_TIME_WINDOW_TAB_T(timeWindowList);
    }

    static List<WaterSupplyAccounting> toWaterSupplyAccountingList(Connection c, WAT_USR_CONTRACT_ACCT_TAB_T
            watUsrContractAcctTabT) {

        List<WaterSupplyAccounting> waterSupplyAccounting = new ArrayList<>();
        Map<AccountingKey, WaterSupplyAccounting> cacheMap = new TreeMap<>();

        for (WAT_USR_CONTRACT_ACCT_OBJ_T watUsrContractAcctObjT : watUsrContractAcctTabT) {
            WATER_USER_CONTRACT_REF_T watUsrContractRef = watUsrContractAcctObjT.getWATER_USER_CONTRACT_REF();
            AccountingKey key = new AccountingKey.Builder()
                .withContractName(watUsrContractRef.getCONTRACT_NAME())
                .withWaterUser(new WaterUser.Builder()
                    .withWaterRight(watUsrContractRef.getWATER_USER().getWATER_RIGHT())
                    .withEntityName(watUsrContractRef.getWATER_USER().getENTITY_NAME())
                    .withProjectId(CwmsId.buildCwmsId(watUsrContractRef.getWATER_USER().getPROJECT_LOCATION_REF().getOFFICE_ID(),
                            watUsrContractRef.getWATER_USER().getPROJECT_LOCATION_REF().call_GET_LOCATION_ID()))
                    .build())
                .build();
            if (cacheMap.containsKey(key)) {
                WaterSupplyAccounting accounting = cacheMap.get(key);
                addTransfer(watUsrContractAcctObjT, accounting);
            } else {
                cacheMap.put(key, createAccounting(c, watUsrContractAcctObjT));
            }
        }
        for (Map.Entry<AccountingKey, WaterSupplyAccounting> entry : cacheMap.entrySet()) {
            waterSupplyAccounting.add(entry.getValue());
        }
        return waterSupplyAccounting;
    }

    private static WaterSupplyAccounting createAccounting(Connection c, WAT_USR_CONTRACT_ACCT_OBJ_T acctObjT) {
        WaterContractDao waterContractDao = new WaterContractDao(DSL.using(c));
        WATER_USER_OBJ_T waterUserObjT = acctObjT.getWATER_USER_CONTRACT_REF().getWATER_USER();
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
            transfer = new PumpTransfer(PumpType.IN, transferDisplay, flow, remarks);
        } else if (pumpOut != null && pumpOut.getName().equalsIgnoreCase(pumpLocation)
                && pumpOut.getOfficeId().equalsIgnoreCase(pumpOffice)) {
            transfer = new PumpTransfer(PumpType.OUT, transferDisplay, flow, remarks);
        } else if (pumpBelow != null && pumpBelow.getName().equalsIgnoreCase(pumpLocation)
                && pumpBelow.getOfficeId().equalsIgnoreCase(pumpOffice)) {
            transfer = new PumpTransfer(PumpType.BELOW, transferDisplay, flow, remarks);
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
                .withPumpBelow(pumpBelow != null ? CwmsId.buildCwmsId(pumpBelow.getOfficeId(), pumpBelow.getName()) : null)
                .build())
            .withPumpAccounting(pumpAccounting)
            .build();
    }

    private static void addTransfer(WAT_USR_CONTRACT_ACCT_OBJ_T acctObjTs, WaterSupplyAccounting accounting) {
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
            transfer = new PumpTransfer(PumpType.IN, transferDisplay, acctObjTs.getPUMP_FLOW(), accountingRemarks);
        } else if (pumpOut != null && pumpOut.getName().equalsIgnoreCase(locationId)
                && pumpOut.getOfficeId().equalsIgnoreCase(officeId)) {
            transfer = new PumpTransfer(PumpType.OUT, transferDisplay, acctObjTs.getPUMP_FLOW(), accountingRemarks);
        } else if (pumpBelow != null && pumpBelow.getName().equalsIgnoreCase(locationId)
                && pumpBelow.getOfficeId().equalsIgnoreCase(officeId)) {
            transfer = new PumpTransfer(PumpType.BELOW, transferDisplay,
                    acctObjTs.getPUMP_FLOW(), accountingRemarks);
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

    static class AccountingKey implements Comparable<AccountingKey> {
        private final WaterUser waterUser;
        private final String contractName;

        private AccountingKey(Builder builder) {
            this.waterUser = builder.waterUser;
            this.contractName = builder.contractName;
        }

        WaterUser getWaterUser() {
            return waterUser;
        }

        String getContractName() {
            return contractName;
        }

        @Override
        public int hashCode() {
            return waterUser.hashCode() + contractName.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof AccountingKey)) {
                return false;
            }
            AccountingKey key = (AccountingKey) obj;
            return compareWaterUser(waterUser, key.getWaterUser()) && key.getContractName().equals(contractName);
        }

        @Override
        public int compareTo(@NotNull AccountingKey key) {
            if (key == this) {
                return 0;
            }
            int i = contractName.compareTo(key.getContractName());
            if (i == 0) {
                i = compareWaterUser(waterUser, key.getWaterUser()) ? 0 : 1;
            }
            return i;
        }

        private boolean compareWaterUser(WaterUser waterUser1, WaterUser waterUser2) {
            return waterUser1.getEntityName().equals(waterUser2.getEntityName())
                    && waterUser1.getProjectId().getName().equals(waterUser2.getProjectId().getName())
                    && waterUser1.getProjectId().getOfficeId().equals(waterUser2.getProjectId().getOfficeId())
                    && waterUser1.getWaterRight().equals(waterUser2.getWaterRight());
        }

        private static final class Builder {
            private WaterUser waterUser;
            private String contractName;

            Builder withWaterUser(WaterUser waterUser) {
                this.waterUser = waterUser;
                return this;
            }

            Builder withContractName(String contractName) {
                this.contractName = contractName;
                return this;
            }

            AccountingKey build() {
                return new AccountingKey(this);
            }
        }
    }
}
