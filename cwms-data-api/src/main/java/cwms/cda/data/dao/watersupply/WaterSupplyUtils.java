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
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.AccountingKey;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserAccountingType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserType;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;
import usace.cwms.db.jooq.dao.util.WaterUserTypeUtil;


final class WaterSupplyUtils {
    private static final Logger LOGGER = Logger.getLogger(WaterSupplyUtils.class.getName());

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

    public static WATER_USER_OBJ_T toWaterUserObjT(WaterUser waterUser) {
        WATER_USER_OBJ_T waterUserObjT = new WATER_USER_OBJ_T();
        waterUserObjT.setENTITY_NAME(waterUser.getEntityName());
        waterUserObjT.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(waterUser.getProjectId()));
        waterUserObjT.setWATER_RIGHT(waterUser.getWaterRight());
        return waterUserObjT;
    }

    public static LOOKUP_TYPE_OBJ_T toLookupTypeO(LookupType lookupType) {
        LOOKUP_TYPE_OBJ_T lookupTypeObjT = new LOOKUP_TYPE_OBJ_T();
        lookupTypeObjT.setOFFICE_ID(lookupType.getOfficeId());
        lookupTypeObjT.setDISPLAY_VALUE(lookupType.getDisplayValue());
        lookupTypeObjT.setTOOLTIP(lookupType.getTooltip());
        lookupTypeObjT.setACTIVE(JooqDao.formatBool(lookupType.getActive()));
        return lookupTypeObjT;
    }

    public static LOOKUP_TYPE_TAB_T toLookupTypeT(LookupType lookupType) {
        List<LOOKUP_TYPE_OBJ_T> lookupTypeList = new ArrayList<>();
        lookupTypeList.add(toLookupTypeO(lookupType));
        return new LOOKUP_TYPE_TAB_T(lookupTypeList);
    }

    public static WATER_USER_CONTRACT_REF_T toContractRef(WaterUser waterUser, String contractName) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUserObjT(waterUser));
        waterUserContractRefT.setCONTRACT_NAME(contractName);
        return waterUserContractRefT;
    }

    public static WATER_USER_CONTRACT_REF_T toWaterUserContractRefTs(WaterUserContract waterUserContract) {
        WATER_USER_CONTRACT_REF_T waterUserContractRefT = new WATER_USER_CONTRACT_REF_T();
        waterUserContractRefT.setWATER_USER(toWaterUserObjT(waterUserContract.getWaterUser()));
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

    public static List<WaterUserAccountingType> toWaterUserAccTypeList(WaterSupplyAccounting accounting,
            WaterUser waterUser, String contractName) {
        List<WaterUserAccountingType> retList = new ArrayList<>();
        if (accounting.getPumpAccounting() != null) {
            for (Map.Entry<Instant, List<PumpTransfer>> pumpIn : accounting.getPumpAccounting().entrySet()) {
                retList.addAll(toWaterUserAccType(pumpIn.getValue(), waterUser, contractName,
                        accounting.getPumpLocations(), pumpIn.getKey()));
            }
        }
        return retList;
    }

    public static List<WaterUserAccountingType> toWaterUserAccType(List<PumpTransfer> accounting, WaterUser user,
            String contractName, PumpLocation pumpLocation, Instant transferDate) {
        List<WaterUserAccountingType> retList = new ArrayList<>();
        for (PumpTransfer transfer : accounting) {
            switch (transfer.getPumpType()) {
                case IN:
                    retList.add(
                        new WaterUserAccountingType(new WaterUserContractRefType(toWaterUserType(user), contractName),
                            new LocationRefType(parseLocationParts(pumpLocation.getPumpIn().getName(), false),
                                parseLocationParts(pumpLocation.getPumpIn().getName(), true),
                                pumpLocation.getPumpIn().getOfficeId()),
                            new usace.cwms.db.dao.ifc.cat.LookupType(pumpLocation.getPumpIn().getOfficeId(),
                                    transfer.getTransferTypeDisplay(), null, true), transfer.getFlow(),
                            new Date(transferDate.toEpochMilli()), transfer.getComment()));
                    break;
                case OUT:
                    retList.add(
                        new WaterUserAccountingType(new WaterUserContractRefType(toWaterUserType(user), contractName),
                            new LocationRefType(parseLocationParts(pumpLocation.getPumpOut().getName(), false),
                                parseLocationParts(pumpLocation.getPumpOut().getName(), true),
                                pumpLocation.getPumpOut().getOfficeId()),
                            new usace.cwms.db.dao.ifc.cat.LookupType(pumpLocation.getPumpOut().getOfficeId(),
                                    transfer.getTransferTypeDisplay(), null, true), transfer.getFlow(),
                            new Date(transferDate.toEpochMilli()), transfer.getComment()));
                    break;
                case BELOW:
                    retList.add(
                        new WaterUserAccountingType(new WaterUserContractRefType(toWaterUserType(user), contractName),
                            new LocationRefType(parseLocationParts(pumpLocation.getPumpBelow().getName(), false),
                                parseLocationParts(pumpLocation.getPumpBelow().getName(), true),
                                pumpLocation.getPumpBelow().getOfficeId()),
                            new usace.cwms.db.dao.ifc.cat.LookupType(pumpLocation.getPumpBelow().getOfficeId(),
                                    transfer.getTransferTypeDisplay(), null, true), transfer.getFlow(),
                            new Date(transferDate.toEpochMilli()), transfer.getComment()));
                    break;
                default:
                    LOGGER.log(Level.CONFIG, "Unknown pump type: {0}", transfer.getPumpType());
                    break;
            }
        }
        return retList;
    }

    public static String parseLocationParts(String location, boolean subLocation) {
        String[] parts = location.split("-");
        int size = parts.length;
        if (subLocation) {
            if (size < 2) {
                return null;
            }
            return parts[1];
        }
        return parts[0];
    }

    public static WaterUserType toWaterUserType(WaterUser waterUser) {
        return new WaterUserType(waterUser.getEntityName(),
                toLocationRefType(waterUser.getProjectId()), waterUser.getWaterRight());
    }

    public static WaterUserType toWaterUserType(WaterUser waterUserType,
            CwmsId projectLocation) {
        return new WaterUserType(waterUserType.getEntityName(),
                toLocationRefType(projectLocation), waterUserType.getWaterRight());
    }

    public static LocationRefType toLocationRefType(CwmsId projectLocation) {
        return new LocationRefType(projectLocation.getName(),null,
                projectLocation.getOfficeId());
    }

    public static usace.cwms.db.dao.ifc.cat.LookupType toLookupType(LookupType lookupType) {
        return new usace.cwms.db.dao.ifc.cat.LookupType(lookupType.getOfficeId(),
                lookupType.getDisplayValue(), lookupType.getTooltip(), lookupType.getActive());
    }

    public static LookupType toLookupType(usace.cwms.db.dao.ifc.cat.LookupType lookupType) {
        return new LookupType.Builder()
                .withOfficeId(lookupType.getOfficeId())
                .withDisplayValue(lookupType.getDisplayValue())
                .withTooltip(lookupType.getTooltip())
                .withActive(lookupType.getActive())
                .build();
    }

    public static WaterUserContractRefType toWaterUserContractRefType(WaterUser user,
            CwmsId projectLocation, String contractName) {
        return new WaterUserContractRefType(toWaterUserType(user,
                projectLocation), contractName);
    }

    public static List<WaterSupplyAccounting> toWaterSupplyAccountingList(Connection c, WAT_USR_CONTRACT_ACCT_TAB_T
            watUsrContractAcctTabT) {

        List<WaterSupplyAccounting> waterSupplyAccounting = new ArrayList<>();
        Map<AccountingKey, WaterSupplyAccounting> cacheMap = new TreeMap<>();

        for (WAT_USR_CONTRACT_ACCT_OBJ_T watUsrContractAcctObjT : watUsrContractAcctTabT) {
            WaterUserAccountingType accounting = WaterUserTypeUtil.toWaterUserAccountingType(watUsrContractAcctObjT);
            WaterContractDao waterContractDao = new WaterContractDao(DSL.using(c));
            WaterUserContract waterUserContract = waterContractDao.getWaterContract(
                accounting.getContractRefTUser().getContractName(),
                new CwmsId.Builder()
                        .withOfficeId(accounting.getContractRefTUser().getWaterUserType().getParentLocationRefType().getOfficeId())
                        .withName(accounting.getContractRefTUser().getWaterUserType().getParentLocationRefType().getBaseLocationId())
                        .build(),
                accounting.getContractRefTUser().getWaterUserType().getEntityName());
            String pumpIn = waterUserContract.getPumpInLocation().getPumpLocation().getName();
            String pumpOut = waterUserContract.getPumpOutLocation().getPumpLocation().getName();
            String pumpBelow = waterUserContract.getPumpOutBelowLocation().getPumpLocation().getName();
            AccountingKey key = new AccountingKey.Builder()
                .withWaterUser(new WaterUser.Builder()
                    .withProjectId(new CwmsId.Builder()
                        .withOfficeId(accounting.getContractRefTUser().getWaterUserType().getParentLocationRefType().getOfficeId())
                        .withName(accounting.getContractRefTUser().getWaterUserType().getParentLocationRefType().getBaseLocationId())
                        .build())
                    .withEntityName(accounting.getContractRefTUser().getWaterUserType().getEntityName())
                    .withWaterRight(accounting.getContractRefTUser().getWaterUserType().getWaterRight()).build())
                .withContractName(accounting.getContractRefTUser().getContractName())
                .build();
            if (cacheMap.containsKey(key)) {
                if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                    ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpIn)
                    : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                    + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpIn)) {

                    if (cacheMap.get(key).getPumpAccounting() != null
                        && cacheMap.get(key).getPumpAccounting().containsKey(accounting.getTransferStartDatetime().toInstant())) {

                        List<PumpTransfer> tempList = cacheMap.get(key).getPumpAccounting()
                                .get(accounting.getTransferStartDatetime().toInstant());
                        List<PumpTransfer> transferList = new ArrayList<>(tempList);
                        transferList.add(new PumpTransfer(PumpType.IN, accounting.getPhysicalXferTYpe().getDisplayValue(),
                            accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(), transferList);
                    } else {
                        List<PumpTransfer> transferList = Collections.singletonList(new PumpTransfer(PumpType.IN,
                                accounting.getPhysicalXferTYpe().getDisplayValue(), accounting.getAccountingFlow(),
                                accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(),
                            transferList);
                    }
                } else if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                        ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpOut)
                        : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                        + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpOut)) {
                    if (cacheMap.get(key).getPumpAccounting().containsKey(accounting.getTransferStartDatetime().toInstant())) {
                        List<PumpTransfer> tempList = cacheMap.get(key).getPumpAccounting().get(accounting.getTransferStartDatetime().toInstant());
                        List<PumpTransfer> transferList = new ArrayList<>(tempList);
                        transferList.add(new PumpTransfer(PumpType.OUT, accounting.getPhysicalXferTYpe().getDisplayValue(),
                                accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(), transferList);
                    } else {
                        List<PumpTransfer> transferList = Collections.singletonList(new PumpTransfer(PumpType.OUT,
                                accounting.getPhysicalXferTYpe().getDisplayValue(), accounting.getAccountingFlow(),
                                accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(),
                            transferList);
                    }
                } else if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                    ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpBelow)
                    : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                    + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpBelow)) {

                    if (cacheMap.get(key).getPumpAccounting().containsKey(accounting.getTransferStartDatetime().toInstant())) {
                        List<PumpTransfer> tempList = cacheMap.get(key).getPumpAccounting().get(accounting.getTransferStartDatetime().toInstant());
                        List<PumpTransfer> transferList = new ArrayList<>(tempList);
                        transferList.add(new PumpTransfer(PumpType.BELOW, accounting.getPhysicalXferTYpe().getDisplayValue(),
                                        accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(), transferList);
                    } else {
                        List<PumpTransfer> transferList = Collections.singletonList(new PumpTransfer(PumpType.BELOW,
                                accounting.getPhysicalXferTYpe().getDisplayValue(), accounting.getAccountingFlow(),
                                accounting.getAccountingRemarks()));
                        cacheMap.get(key).getPumpAccounting().put(accounting.getTransferStartDatetime().toInstant(),
                            transferList);
                    }
                }
            } else {
                List<PumpTransfer> transferList = new ArrayList<>();
                Map<Instant, List<PumpTransfer>> timeMap = new TreeMap<>();

                if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                        ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpIn)
                        : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                        + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpIn)) {
                    transferList.add(new PumpTransfer(PumpType.IN, accounting.getPhysicalXferTYpe().getDisplayValue(),
                            accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                } else if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                        ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpOut)
                        : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                        + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpOut)) {
                    transferList.add(new PumpTransfer(PumpType.OUT, accounting.getPhysicalXferTYpe().getDisplayValue(),
                            accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                } else if (accounting.getPumpLocationRefTUser().getSubLocationId() == null
                        ? accounting.getPumpLocationRefTUser().getBaseLocationId().equals(pumpBelow)
                        : (accounting.getPumpLocationRefTUser().getBaseLocationId()
                        + "-" + accounting.getPumpLocationRefTUser().getSubLocationId()).equals(pumpBelow)) {
                    transferList.add(new PumpTransfer(PumpType.BELOW, accounting.getPhysicalXferTYpe().getDisplayValue(),
                            accounting.getAccountingFlow(), accounting.getAccountingRemarks()));
                }
                timeMap.put(accounting.getTransferStartDatetime().toInstant(), transferList);


                WaterSupplyAccounting supplyAccounting = new WaterSupplyAccounting.Builder()
                    .withPumpAccounting(timeMap)
                    .withContractName(key.getContractName())
                    .withWaterUser(key.getWaterUser())
                        .withPumpLocations(new PumpLocation.Builder()
                            .withPumpIn(CwmsId.buildCwmsId(waterUserContract.getPumpInLocation().getPumpLocation().getOfficeId(),
                                    waterUserContract.getPumpInLocation().getPumpLocation().getName()))
                            .withPumpOut(CwmsId.buildCwmsId(waterUserContract.getPumpOutLocation().getPumpLocation().getOfficeId(),
                                    waterUserContract.getPumpOutLocation().getPumpLocation().getName()))
                            .withPumpBelow(CwmsId.buildCwmsId(waterUserContract.getPumpOutBelowLocation().getPumpLocation().getOfficeId(),
                                    waterUserContract.getPumpOutBelowLocation().getPumpLocation().getName()))
                            .build())
                    .build();
                cacheMap.put(key, supplyAccounting);
            }
        }

        for (Map.Entry<AccountingKey, WaterSupplyAccounting> entry : cacheMap.entrySet()) {
            waterSupplyAccounting.add(entry.getValue());
        }
        return waterSupplyAccounting;
    }
}
