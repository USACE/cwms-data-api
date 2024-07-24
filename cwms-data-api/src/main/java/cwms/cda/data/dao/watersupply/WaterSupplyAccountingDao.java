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
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyPumpAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import hec.data.TimeWindow;
import hec.data.TimeWindowMap;
import hec.lang.Const;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import rma.util.RMAConst;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.watersupply.TimeWindowType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;
import usace.cwms.db.jooq.dao.util.WaterUserTypeUtil;

public class WaterSupplyAccountingDao extends JooqDao<WaterSupplyPumpAccounting> {
    public WaterSupplyAccountingDao(DSLContext dsl) {
        super(dsl);
    }

    public WaterSupplyAccounting retrieveAccounting(String contractName, WaterUser waterUser, Instant startTime,
            Instant endTime, boolean startInclusive, boolean endInclusive, boolean headFlag, int rowLimit) {
        WaterSupplyAccounting accounting = new WaterSupplyAccounting.Builder().withContractName(contractName)
                .withWaterUser(waterUser).withPumpTimeWindowMap(new HashMap<>())
                .withPumpLocationMap(new HashMap<>()).build();
        TimeZone timeZone = null;
        String transferType = null;

        List<WaterSupplyPumpAccounting> waterSupplyPumpAccountingList = retrieveAccountingSet(contractName, waterUser,
                waterUser.getProjectId(), null, startTime, endTime, timeZone, startInclusive,
                endInclusive, headFlag, rowLimit, transferType);
        buildAccountingMap(waterSupplyPumpAccountingList, accounting);
        return accounting;
    }

    public void storeAccounting(WaterSupplyAccounting accounting) {
        List<WaterSupplyPumpAccounting> accountingList = new ArrayList<>();
        Map<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> allPumpAccounting
                = accounting.getAllPumpAccounting();
        Set<Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>>> entrySet
                = allPumpAccounting.entrySet();

        List<TimeWindowType> timeWindowTypes = new ArrayList<>();
        for (Map.Entry<CwmsId, NavigableMap<Instant, WaterSupplyPumpAccounting>> entry : entrySet) {
            CwmsId pumpLocation = entry.getKey();
            LocationRefType pumpLocRef = new LocationRefType(pumpLocation.getName(), null,
                    pumpLocation.getOfficeId());
            NavigableMap<Instant, WaterSupplyPumpAccounting> pumpAccountingMap = entry.getValue();
            TimeWindowMap timeWindowMap = accounting.getTimeWindowMap(pumpLocation);
            if (timeWindowMap == null) {
                continue;
            }
            Set<TimeWindow> timeWindowSet = timeWindowMap.getTimeWindowSet();
            for (TimeWindow tw : timeWindowSet) {
                Instant twStartDate = tw.getStartDate().toInstant();
                Instant twEndDate = tw.getEndDate().toInstant();
                TimeWindowType timeWindowType = new TimeWindowType(pumpLocRef, new Date(twStartDate.toEpochMilli()),
                        new Date(twEndDate.toEpochMilli()));
                timeWindowTypes.add(timeWindowType);
                NavigableMap<Instant, WaterSupplyPumpAccounting> subMap
                        = pumpAccountingMap.subMap(twStartDate, true, twEndDate, true);
                Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.INFO,
                        "pump:{0} time window: {1}, {2}", new Object[]{pumpLocRef.getBaseLocationId(),
                            twStartDate, twEndDate});
                Collection<WaterSupplyPumpAccounting> accountings = subMap.values();
                accountingList.addAll(buildWaterContractAccountingTypes(accounting.getContractName(),
                        accounting.getWaterUser(), pumpLocRef, accountings));
            }
        }
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
        String volumeUnitId = null;
        String storeRule = Const.Delete_Insert;
        boolean overrideProtection = false;

        storeAccountingSet(accountingList, accounting.getContractName(), accounting.getWaterUser(), timeWindowTypes,
                timeZone, volumeUnitId, storeRule, overrideProtection);
    }

    public List<WaterSupplyPumpAccounting> retrieveAccountingSet(String contractName, WaterUser waterUser,
            CwmsId projectLocation, String units, Instant startTime, Instant endTime, TimeZone timeZone,
            boolean startInclusive, boolean endInclusive, boolean ascendingFlag, int rowLimit, String transferType) {

        WATER_USER_CONTRACT_REF_T contractRefT = WaterUserTypeUtil.toWaterUserContractReft(WaterSupplyUtils
                .map(waterUser, projectLocation, contractName));
        Timestamp startTimestamp = OracleTypeMap.buildTimestamp(new Date(startTime.toEpochMilli()));
        Timestamp endTimestamp = OracleTypeMap.buildTimestamp(new Date(endTime.toEpochMilli()));
        String timeZoneId = timeZone == null ? null : timeZone.getID();
        String startInclusiveFlag = OracleTypeMap.formatBool(startInclusive);
        String endInclusiveFlag = OracleTypeMap.formatBool(endInclusive);
        String ascendingFlagStr = OracleTypeMap.formatBool(ascendingFlag);
        BigInteger rowLimitBigInt = BigInteger.valueOf(rowLimit);

        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            WAT_USR_CONTRACT_ACCT_TAB_T watUsrContractAcctObjTs
                    = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_ACCOUNTING_SET(DSL.using(c).configuration(),
                    contractRefT, units, startTimestamp, endTimestamp, timeZoneId, startInclusiveFlag,
                    endInclusiveFlag, ascendingFlagStr, rowLimitBigInt, transferType);
            if (watUsrContractAcctObjTs != null) {
                return watUsrContractAcctObjTs.stream()
                        .map(WaterSupplyUtils::map)
                        .collect(Collectors.toList());
            } else {
                return new ArrayList<>();
            }
        });
    }

    public void storeAccountingSet(List<WaterSupplyPumpAccounting> accountingList, String contractName,
            WaterUser waterUser, List<TimeWindowType> timeWindowTypes, TimeZone timeZone, String volumeUnitId,
            String storeRule, boolean overrideProtection) {

        connection(dsl, c -> {
            setOffice(c, waterUser.getProjectId().getOfficeId());
            WAT_USR_CONTRACT_ACCT_TAB_T accountingTab = WaterUserTypeUtil.toWaterUserContractAcctTs(WaterSupplyUtils
                    .map(accountingList));
            WATER_USER_CONTRACT_REF_T contractRefT = WaterUserTypeUtil.toWaterUserContractReft(WaterSupplyUtils
                    .map(waterUser, waterUser.getProjectId(), contractName));
            LOC_REF_TIME_WINDOW_TAB_T pumpTimeWindowTab = WaterUserTypeUtil.toLocRefTimeWindowTs(timeWindowTypes);
            String timeZoneId = timeZone == null ? null : timeZone.getID();
            String overrideProt = OracleTypeMap.formatBool(overrideProtection);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_ACCOUNTING_SET(DSL.using(c).configuration(), accountingTab,
                    contractRefT, pumpTimeWindowTab, timeZoneId, volumeUnitId, storeRule, overrideProt);
        });
    }

    public void buildAccountingMap(List<WaterSupplyPumpAccounting> accountingList, WaterSupplyAccounting accounting) {
        for (WaterSupplyPumpAccounting pumpAccounting : accountingList) {
            if (pumpAccounting == null || !compareWaterUsers(accounting.getWaterUser(), pumpAccounting.getWaterUser())
                   || !accounting.getContractName().equals(pumpAccounting.getContractName())) {
                continue;
            }
            CwmsId pumpLocationRef = pumpAccounting.getPumpLocation();
            Instant transferStartDate = pumpAccounting.getTransferDate();
            NavigableMap<Instant, WaterSupplyPumpAccounting> pumpAccountingMap
                   = accounting.buildPumpAccounting(pumpLocationRef);
            pumpAccountingMap.put(transferStartDate, pumpAccounting);

            Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.INFO,"pump:{0} date:{1} val:{2}",
                   new Object[] {
                       pumpLocationRef,
                       pumpAccounting.getTransferDate(),
                       Double.toString(pumpAccounting.getFlow())
                   });
        }
        accounting.clearPumpTimeWindowMaps();
    }

    private boolean compareWaterUsers(WaterUser first, WaterUser second) {
        return first != null && second != null
                && first.getEntityName().equals(second.getEntityName())
                && first.getProjectId().getName().equals(second.getProjectId().getName())
                && first.getProjectId().getOfficeId().equals(second.getProjectId().getOfficeId())
                && first.getWaterRight().equals(second.getWaterRight());
    }

    private List<WaterSupplyPumpAccounting> buildWaterContractAccountingTypes(String contractName, WaterUser waterUser,
            LocationRefType pumpLocationRef, Collection<WaterSupplyPumpAccounting> accountings) {
        List<WaterSupplyPumpAccounting> retVal = new ArrayList<>();
        for (WaterSupplyPumpAccounting accounting: accountings) {
            if (!RMAConst.isValidValue(accounting.getFlow())) {
                continue;
            }
            LookupType transferType = accounting.getTransferType();
            LookupType physicalXferType = null;
            if (transferType != null) {
                physicalXferType = new LookupType.Builder().withOfficeId(transferType.getOfficeId())
                        .withDisplayValue(transferType.getDisplayValue()).withActive(transferType.getActive())
                        .withTooltip(transferType.getTooltip()).build();
            }

            double flow = accounting.getFlow();
            Double accountingFlow = flow;
            Instant transferStartDatetime = accounting.getTransferDate();

            String accountingRemarks = accounting.getComment();

            Logger.getLogger(WaterSupplyAccounting.class.getName()).log(Level.INFO,
                "pump:{0} date:{1} val:{2}", new Object[]
                    {
                            pumpLocationRef.getBaseLocationId(),
                            accounting.getTransferDate(),
                            Double.toString(flow)
                    });
            WaterSupplyPumpAccounting waterSupplyPumpAccounting = new WaterSupplyPumpAccounting.Builder()
                    .withWaterUser(waterUser).withContractName(contractName)
                    .withPumpLocation(new CwmsId.Builder().withName(pumpLocationRef.getBaseLocationId())
                            .withOfficeId(pumpLocationRef.getOfficeId()).build())
                    .withTransferType(physicalXferType).withFlow(accountingFlow)
                    .withTransferDate(transferStartDatetime).withComment(accountingRemarks).build();
            retVal.add(waterSupplyPumpAccounting);
        }
        return retVal;
    }
}
