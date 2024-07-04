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
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.data.dto.watersupply.WaterUserContractRef;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_TAB_T;
import usace.cwms.db.jooq.dao.util.LocationTypeUtil;
import usace.cwms.db.jooq.dao.util.LookupTypeUtil;
import usace.cwms.db.jooq.dao.util.WaterUserTypeUtil;

import static java.util.stream.Collectors.toList;

public class WaterContractDao extends JooqDao<WaterUserContract> {
    public WaterContractDao(DSLContext dsl) {
        super(dsl);
    }

    public List<WaterUserContract> getAllWaterContracts(CwmsId projectLocation, String entityName) {
        List<WaterUserContract> retVal = new ArrayList<>();
        List<WaterUserContractType> waterUserContractTypes = new ArrayList<>();

        LocationRefType locationRefType = WaterSupplyUtils.map(projectLocation);

        waterUserContractTypes.addAll(connectionResult(dsl, c -> {
            LOCATION_REF_T pProjectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            WATER_USER_CONTRACT_TAB_T waterUserContractObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_CONTRACTS(
                    DSL.using(c).configuration(), pProjectLocationRef, entityName);
            return waterUserContractObjTs.stream()
                    .map(WaterUserTypeUtil::toWaterUserContractType)
                    .collect(toList());
            })
        );

        for (WaterUserContractType waterUserContractType : waterUserContractTypes) {
            WaterUserContract waterUserContract = new WaterUserContract.Builder()
                    .withWaterContract(WaterSupplyUtils.map(waterUserContractType.getWaterSupplyContractType()))
                    .withWaterContract(WaterSupplyUtils.map(waterUserContractType.getWaterSupplyContractType()))
                    .withContractEffectiveDate(waterUserContractType.getContractEffectiveDate())
                    .withContractExpirationDate(waterUserContractType.getContractExpirationDate())
                    .withContractedStorage(waterUserContractType.getContractedStorage())
                    .withInitialUseAllocation(waterUserContractType.getInitialUseAllocation())
                    .withFutureUseAllocation(waterUserContractType.getFutureUseAllocation())
                    .withStorageUnitsId(waterUserContractType.getStorageUnitsId())
                    .withFutureUsePercentActivated(waterUserContractType.getFutureUsePercentActivated())
                    .withTotalAllocPercentActivated(waterUserContractType.getTotalAllocPercentActivated())
                    .withPumpOutLocation(WaterSupplyUtils.map(waterUserContractType.getPumpOutLocation()))
                    .withPumpOutBelowLocation(WaterSupplyUtils.map(waterUserContractType.getPumpOutBelowLocation()))
                    .withPumpInLocation(WaterSupplyUtils.map(waterUserContractType.getPumpInLocation()))
                    .build();
            retVal.add(waterUserContract);
        }
        return retVal;
    }

    public List<LookupType> getAllWaterContractTypes(String officeId) {
        List<LookupType> retVal = new ArrayList<>();
        List<usace.cwms.db.dao.ifc.cat.LookupType> contractTypeList = connectionResult(dsl, c -> {
            LOOKUP_TYPE_TAB_T lookupTypeObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_GET_CONTRACT_TYPES(
                    DSL.using(c).configuration(), officeId);
            return lookupTypeObjTs.stream()
                    .map(LookupTypeUtil::toLookupType)
                    .collect(toList());

        });
        for (usace.cwms.db.dao.ifc.cat.LookupType lookupType : contractTypeList) {
            LookupType waterUserContractType = WaterSupplyUtils.map(lookupType);
            retVal.add(waterUserContractType);
        }
        return retVal;
    }

    public List<WaterUser> getAllWaterUsers(CwmsId projectLocation) {
        List<WaterUser> retVal = new ArrayList<>();
        LocationRefType locationRefType = WaterSupplyUtils.map(projectLocation);
        List<WaterUserType> waterUserTypes = connectionResult(dsl, c -> {
                LOCATION_REF_T pProjectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
                WATER_USER_TAB_T waterUserObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                        DSL.using(c).configuration(), pProjectLocationRef);
                return waterUserObjTs.stream()
                        .map(WaterUserTypeUtil::toWaterUserType)
                        .collect(toList());
        });
        for (WaterUserType waterUserType : waterUserTypes) {
            WaterUser waterUser = WaterSupplyUtils.map(waterUserType, projectLocation);
            retVal.add(waterUser);
        }
        return retVal;
    }

    public WaterUser getWaterUser(CwmsId projectLocation, String entityName) {
        LocationRefType locationRefType = WaterSupplyUtils.map(projectLocation);
        List<WaterUserType> userList = connectionResult(dsl, c -> {
            LOCATION_REF_T pProjectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            WATER_USER_TAB_T waterUserObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                    DSL.using(c).configuration(), pProjectLocationRef);
            return waterUserObjTs.stream()
                    .map(WaterUserTypeUtil::toWaterUserType)
                    .collect(toList());
        });
        for (WaterUserType waterUser : userList) {
            if (waterUser.getEntityName().equals(entityName)) {
                return WaterSupplyUtils.map(waterUser, projectLocation);
            }
        }
        return null;
    }

    public void storeWaterContract(WaterUserContract waterContractType, boolean failIfExists, boolean ignoreNulls) {
        List<WaterUserContractType> waterUserContractTypeModified = new ArrayList<>();
        waterUserContractTypeModified.add(WaterSupplyUtils.map(waterContractType));

        connection(dsl, c -> {
            String pFailIfExists = OracleTypeMap.formatBool(failIfExists);
            String pIgnoreNulls = OracleTypeMap.formatBool(ignoreNulls);
            WATER_USER_CONTRACT_TAB_T pContracts = WaterUserTypeUtil
                    .toWaterUserContractTs(waterUserContractTypeModified);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_CONTRACTS2(DSL.using(c).configuration(), pContracts,
                    pFailIfExists, pIgnoreNulls);
        });
    }

    public void renameWaterUser(String oldWaterUser, String newWaterUser, CwmsId projectLocation) {

        LocationRefType locationRefType = WaterSupplyUtils.map(projectLocation);

        connection(dsl, c -> {
            LOCATION_REF_T pProjectLocationRefT =  LocationTypeUtil.toLocationRefT(locationRefType);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_WATER_USER(DSL.using(c).configuration(), pProjectLocationRefT,
                    oldWaterUser, newWaterUser);
        });
    }

    public void storeWaterUser(WaterUser waterUser, boolean failIfExists) {

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = WaterSupplyUtils.map(waterUser.getParentLocationRef());

        List<WaterUserType> waterUserTypeModified = new ArrayList<>();
        waterUserTypeModified.add(new usace.cwms.db.dao.ifc.watersupply.WaterUserType(waterUser.getEntityName(),
                locationRefType, waterUser.getWaterRight()));

        connection(dsl, c -> {
            WATER_USER_TAB_T pWaterUsers = WaterUserTypeUtil.toWaterUserTs(waterUserTypeModified);
            String pFailIfExists = OracleTypeMap.formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_WATER_USERS(DSL.using(c).configuration(), pWaterUsers, pFailIfExists);
        });
    }

    public void renameWaterContract(WaterUserContractRef waterContractRefType, String oldContractName,
            String newContractName) {

        WaterUserContractRefType waterUserContractRefType =
                WaterSupplyUtils.map(waterContractRefType, waterContractRefType.getWaterUser().getParentLocationRef());

        connection(dsl, c -> {
            WATER_USER_OBJ_T waterUser = WaterUserTypeUtil.toWaterUserT(waterUserContractRefType.getWaterUserType());
            String contractName = waterUserContractRefType.getContractName();
            WATER_USER_CONTRACT_REF_T pWaterUserContract = new WATER_USER_CONTRACT_REF_T(waterUser, contractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_CONTRACT(DSL.using(c).configuration(), pWaterUserContract,
                    oldContractName,
                    newContractName);
        });
    }

    public void deleteWaterUser(CwmsId location, String entityName, String deleteAction) {

        LocationRefType locationRefType = WaterSupplyUtils.map(location);

        connection(dsl, c -> {
            LOCATION_REF_T pProjectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_WATER_USER(DSL.using(c).configuration(), pProjectLocationRef,
                    entityName, deleteAction);
        });
    }

    public void deleteWaterContract(WaterUserContract contract, String deleteAction) {

        WaterUserContractRef waterUserContractRefType = new WaterUserContractRef(contract.getWaterUser(), contract.getContractId().getName());

        WaterUserContractRefType waterUserContractRefTypeModified =
                WaterSupplyUtils.map(waterUserContractRefType, waterUserContractRefType.getWaterUser().getParentLocationRef());

        connection(dsl, c -> {
            WATER_USER_OBJ_T waterUser = WaterUserTypeUtil.toWaterUserT(waterUserContractRefTypeModified
                    .getWaterUserType());
            String contractName = waterUserContractRefType.getContractName();
            WATER_USER_CONTRACT_REF_T pWaterUserContract = new WATER_USER_CONTRACT_REF_T(waterUser, contractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_CONTRACT(DSL.using(c).configuration(), pWaterUserContract,
                    deleteAction);
        });
    }

    public void storeWaterContractTypes(List<LookupType> lookupTypes,
            boolean failIfExists) {

        List<usace.cwms.db.dao.ifc.cat.LookupType> lookups = new ArrayList<>();

        for (LookupType lookupType : lookupTypes) {
            lookups.add(WaterSupplyUtils.map(lookupType));
        }
        connection(dsl, c -> {
            LOOKUP_TYPE_TAB_T pContractTypes = LookupTypeUtil.buildLookupTypeTab(lookups);
            String pFailIfExists = OracleTypeMap.formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_SET_CONTRACT_TYPES(DSL.using(c).configuration(), pContractTypes, pFailIfExists);
        });
    }

    public void removePumpFromContract(WaterUserContract contract, String pumpLocId,
            String usageId, boolean deleteAccountingData) {

        WaterUserContractRefType contractRefType = new WaterUserContractRefType(WaterSupplyUtils.map(contract.getWaterUser()),
                contract.getContractId().getName());
        connection(dsl, c -> {
            WATER_USER_CONTRACT_REF_T waterUserContractRefT = WaterUserTypeUtil
                    .toWaterUserContractReft(contractRefType);
            String pDeleteAccountingData = OracleTypeMap.formatBool(deleteAccountingData);
            CWMS_WATER_SUPPLY_PACKAGE.call_DISASSOCIATE_PUMP(DSL.using(c).configuration(),
                    waterUserContractRefT, pumpLocId, usageId, pDeleteAccountingData);
        });
    }
}
