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
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import hec.lang.Const;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jspecify.annotations.NonNull;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T;


public class WaterSupplyAccountingDao extends JooqDao<WaterSupplyAccounting> {

    public WaterSupplyAccountingDao(DSLContext dsl) {
        super(dsl);
    }

    public void storeAccounting(WaterSupplyAccounting accounting) {
        String volumeUnitId = null;
        String storeRule = Const.Delete_Insert;
        boolean overrideProtection = false;

        connection(dsl, c -> {
            setOffice(c, accounting.getWaterUser().getProjectId().getOfficeId());
            try {
                storeViaCodegen(c, accounting, overrideProtection, volumeUnitId, storeRule);
            } catch (DataAccessException e) {
                if (isBindException(e)) {
                    try {
                        storeViaManual(c, accounting, overrideProtection, volumeUnitId, storeRule);
                        return;
                    } catch (Exception e2) {
                        RuntimeException re = new RuntimeException(e);
                        re.addSuppressed(e2);
                        throw re;
                    }
                }
                throw e;
            }
        });
    }

    private static boolean isBindException(DataAccessException e) {
        boolean isBind = false;
        Throwable cause = e.getCause();
        if( cause instanceof SQLException){
            SQLException se = (SQLException)cause;
            String sqlMessage = se.getMessage();
            Throwable sqlCause = se.getCause();
            isBind = (sqlCause instanceof IllegalArgumentException || sqlCause instanceof ArrayIndexOutOfBoundsException)
                && sqlMessage.contains("Error while writing value");
        }
        return isBind;
    }

    private static void storeViaCodegen(Connection c, WaterSupplyAccounting accounting, boolean overrideProtection, String volumeUnitId, String storeRule) {
        WAT_USR_CONTRACT_ACCT_TAB_T accountingTab = WaterSupplyUtils.toWaterUserContractAcctTs(accounting);
        WATER_USER_CONTRACT_REF_T contractRefT = WaterSupplyUtils
                .toContractRef(accounting.getWaterUser(), accounting.getContractName());
        LOC_REF_TIME_WINDOW_TAB_T pumpTimeWindowTab = WaterSupplyUtils.toTimeWindowTabT(accounting);
        String timeZoneId = "UTC";
        String overrideProt = formatBool(overrideProtection);
        CWMS_WATER_SUPPLY_PACKAGE.call_STORE_ACCOUNTING_SET(DSL.using(c).configuration(), accountingTab,
                contractRefT, pumpTimeWindowTab, timeZoneId, volumeUnitId, storeRule, overrideProt);
    }

    private static void storeViaManual(Connection c, WaterSupplyAccounting accounting, boolean overrideProtection, String volumeUnitId, String storeRule) {
        var accountingTab = WaterSupplyUtilsLegacy.toManualWaterUserContractAcctTs(accounting);
        var contractRefT = WaterSupplyUtilsLegacy.toContractRef(accounting.getWaterUser(), accounting.getContractName());
        var pumpTimeWindowTab = WaterSupplyUtilsLegacy.toTimeWindowTabT(accounting);
        String timeZoneId = "UTC";
        String overrideProt = formatBool(overrideProtection);
        usace.cwms.db.jooq.codegen_legacy.packages.CWMS_WATER_SUPPLY_PACKAGE.call_STORE_ACCOUNTING_SET(DSL.using(c).configuration(), accountingTab,
                contractRefT, pumpTimeWindowTab, timeZoneId, volumeUnitId, storeRule, overrideProt);
    }

    public List<WaterSupplyAccounting> retrieveAccounting(String contractName, WaterUser waterUser,
            CwmsId projectLocation, String units, Instant startTime, Instant endTime,
            boolean startInclusive, boolean endInclusive, boolean ascendingFlag, int rowLimit) {

        String transferType = null;
        Timestamp startTimestamp = Timestamp.from(startTime);
        Timestamp endTimestamp = Timestamp.from(endTime);
        String timeZoneId = "UTC";
        String startInclusiveFlag = formatBool(startInclusive);
        String endInclusiveFlag = formatBool(endInclusive);
        String ascendingFlagStr = formatBool(ascendingFlag);
        BigInteger rowLimitBigInt = BigInteger.valueOf(rowLimit);

        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            try {
                var contractRefT = WaterSupplyUtils.toContractRef(waterUser, contractName);
                return retrieveFromCodegen(units, c, contractRefT, startTimestamp, endTimestamp, timeZoneId, startInclusiveFlag, endInclusiveFlag, ascendingFlagStr, rowLimitBigInt, transferType);
            } catch (DataAccessException e){
                if(isInvalidColumn(e)){
                    var contractRefT = WaterSupplyUtilsLegacy.toContractRef(waterUser, contractName);
                    return retrieveViaManual(units, c, contractRefT, startTimestamp, endTimestamp, timeZoneId, startInclusiveFlag, endInclusiveFlag, ascendingFlagStr, rowLimitBigInt, transferType);
                }
                throw e;
            }
        });
    }

    private @NonNull List<WaterSupplyAccounting> retrieveViaManual(String units, Connection c, usace.cwms.db.jooq.codegen_legacy.udt.records.WATER_USER_CONTRACT_REF_T contractRefT, Timestamp startTimestamp, Timestamp endTimestamp, String timeZoneId, String startInclusiveFlag, String endInclusiveFlag, String ascendingFlagStr, BigInteger rowLimitBigInt, String transferType) {
        usace.cwms.db.jooq.codegen_legacy.udt.records.WAT_USR_CONTRACT_ACCT_TAB_T watUsrContractAcctObjTs
                = usace.cwms.db.jooq.codegen_legacy.packages.CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_ACCOUNTING_SET(DSL.using(c).configuration(),
                contractRefT, units, startTimestamp, endTimestamp, timeZoneId, startInclusiveFlag,
                endInclusiveFlag, ascendingFlagStr, rowLimitBigInt, transferType);
        if (!watUsrContractAcctObjTs.isEmpty()) {
            return WaterSupplyUtilsLegacy.toWaterSupplyAccountingList(c, watUsrContractAcctObjTs, units);
        } else {
            return new ArrayList<>();
        }
    }

    private boolean isInvalidColumn(DataAccessException e) {
        boolean isInvalid = false;

        Throwable cause = e.getCause();
        if( cause instanceof SQLException){
            SQLException se = (SQLException)cause;
            String sqlMessage = se.getMessage();
            isInvalid =  sqlMessage.contains("Invalid column");
        }

        return isInvalid;
    }

    private static @NonNull List<WaterSupplyAccounting> retrieveFromCodegen(String units, Connection c, WATER_USER_CONTRACT_REF_T contractRefT, Timestamp startTimestamp, Timestamp endTimestamp, String timeZoneId, String startInclusiveFlag, String endInclusiveFlag, String ascendingFlagStr, BigInteger rowLimitBigInt, String transferType) {
        WAT_USR_CONTRACT_ACCT_TAB_T watUsrContractAcctObjTs
            = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_ACCOUNTING_SET(DSL.using(c).configuration(),
                contractRefT, units, startTimestamp, endTimestamp, timeZoneId, startInclusiveFlag,
                endInclusiveFlag, ascendingFlagStr, rowLimitBigInt, transferType);
        if (!watUsrContractAcctObjTs.isEmpty()) {
            return WaterSupplyUtils.toWaterSupplyAccountingList(c, watUsrContractAcctObjTs);
        } else {
            return new ArrayList<>();
        }
    }
}
