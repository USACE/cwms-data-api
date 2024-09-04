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
import cwms.cda.data.dto.watersupply.PumpAccounting;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import hec.lang.Const;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.watersupply.TimeWindowType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;
import usace.cwms.db.jooq.dao.util.WaterUserTypeUtil;


public class WaterSupplyAccountingDao extends JooqDao<WaterSupplyAccounting> {
    public WaterSupplyAccountingDao(DSLContext dsl) {
        super(dsl);
    }

    public void storeAccounting(WaterSupplyAccounting accounting) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
        String volumeUnitId = null;
        String storeRule = Const.Delete_Insert;
        boolean overrideProtection = false;

        connection(dsl, c -> {
            setOffice(c, accounting.getWaterUser().getProjectId().getOfficeId());

            WAT_USR_CONTRACT_ACCT_TAB_T accountingTab = WaterUserTypeUtil.toWaterUserContractAcctTs(WaterSupplyUtils
                    .toWaterUserAccTypeList(accounting, accounting.getWaterUser(), accounting.getContractName()));
            WATER_USER_CONTRACT_REF_T contractRefT = WaterUserTypeUtil.toWaterUserContractReft(WaterSupplyUtils
                    .toWaterUserContractRefType(accounting.getWaterUser(), accounting.getWaterUser().getProjectId(),
                            accounting.getContractName()));
            LOC_REF_TIME_WINDOW_TAB_T pumpTimeWindowTab = WaterUserTypeUtil
                    .toLocRefTimeWindowTs(getTimeWindowTypeList(accounting));
            String timeZoneId = timeZone == null ? null : timeZone.getID();
            String overrideProt = OracleTypeMap.formatBool(overrideProtection);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_ACCOUNTING_SET(DSL.using(c).configuration(), accountingTab,
                    contractRefT, pumpTimeWindowTab, timeZoneId, volumeUnitId, storeRule, overrideProt);
        });
    }

    public List<WaterSupplyAccounting> retrieveAccounting(String contractName, WaterUser waterUser,
            CwmsId projectLocation, String units, Instant startTime, Instant endTime,
            boolean startInclusive, boolean endInclusive, boolean ascendingFlag, int rowLimit) {

        String transferType = null;
        WATER_USER_CONTRACT_REF_T contractRefT = WaterUserTypeUtil.toWaterUserContractReft(WaterSupplyUtils
                .toWaterUserContractRefType(waterUser, projectLocation, contractName));
        Timestamp startTimestamp = OracleTypeMap.buildTimestamp(new Date(startTime.toEpochMilli()));
        Timestamp endTimestamp = OracleTypeMap.buildTimestamp(new Date(endTime.toEpochMilli()));
        String timeZoneId = null;
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
            if (!watUsrContractAcctObjTs.isEmpty()) {
                return WaterSupplyUtils.toWaterSupplyAccountingList(c, watUsrContractAcctObjTs);
            } else {
                return new ArrayList<>();
            }
        });
    }

    private List<TimeWindowType> getTimeWindowTypeList(WaterSupplyAccounting accounting) {
        List<TimeWindowType> retList = new ArrayList<>();
        if (accounting.getPumpInAccounting() != null) {
            for (PumpAccounting pumpIn : accounting.getPumpInAccounting().values()) {
                LocationRefType locationRefType = new LocationRefType(pumpIn.getPumpLocation().getName(),
                        null, pumpIn.getPumpLocation().getOfficeId());
                for (PumpTransfer transfer : pumpIn.getPumpTransfers().values()) {
                    retList.add(new TimeWindowType(locationRefType, new Date(transfer.getTransferDate().toEpochMilli()),
                            new Date(transfer.getTransferDate().toEpochMilli())));
                }
            }
        }
        if (accounting.getPumpOutAccounting() != null) {
            for (PumpAccounting pumpOut : accounting.getPumpOutAccounting().values()) {
                LocationRefType locationRefType = new LocationRefType(pumpOut.getPumpLocation().getName(),
                        null, pumpOut.getPumpLocation().getOfficeId());
                for (PumpTransfer transfer : pumpOut.getPumpTransfers().values()) {
                    retList.add(new TimeWindowType(locationRefType, new Date(transfer.getTransferDate().toEpochMilli()),
                            new Date(transfer.getTransferDate().toEpochMilli())));
                }
            }
        }
        if (accounting.getPumpBelowAccounting() != null) {
            for (PumpAccounting pumpBelow : accounting.getPumpBelowAccounting().values()) {
                LocationRefType locationRefType = new LocationRefType(pumpBelow.getPumpLocation().getName(),
                        null, pumpBelow.getPumpLocation().getOfficeId());
                for (PumpTransfer transfer : pumpBelow.getPumpTransfers().values()) {
                    retList.add(new TimeWindowType(locationRefType, new Date(transfer.getTransferDate().toEpochMilli()),
                            new Date(transfer.getTransferDate().toEpochMilli())));
                }
            }
        }
        return retList;
    }
}
