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

import static java.util.stream.Collectors.toList;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
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



public class WaterContractDao extends JooqDao<WaterUserContract> {
    public WaterContractDao(DSLContext dsl) {
        super(dsl);
    }

    public List<WaterUserContract> getAllWaterContracts(CwmsId projectLocation, String entityName) {
        List<WaterUserContract> retVal = new ArrayList<>();
        List<WaterUserContractType> waterUserContractTypes = new ArrayList<>();

        LocationRefType locationRefType = WaterSupplyUtils.map(projectLocation);

        waterUserContractTypes.addAll(connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            WATER_USER_CONTRACT_TAB_T waterUserContractObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_CONTRACTS(
                    DSL.using(c).configuration(), projectLocationRef, entityName);
            return waterUserContractObjTs.stream()
                    .map(WaterUserTypeUtil::toWaterUserContractType)
                    .collect(toList());
        }));

        for (WaterUserContractType waterUserContractType : waterUserContractTypes) {
            WaterUserContract waterUserContract = new WaterUserContract.Builder()
                    .withContractType(WaterSupplyUtils.map(waterUserContractType.getWaterSupplyContractType()))
                    .withWaterUser(WaterSupplyUtils.map(waterUserContractType
                            .getWaterUserContractRefType()
                            .getWaterUserType(),
                            projectLocation))
                    .withContractEffectiveDate(waterUserContractType.getContractEffectiveDate())
                    .withOfficeId(waterUserContractType.getWaterSupplyContractType().getOfficeId())
                    .withContractId(new CwmsId.Builder()
                            .withOfficeId(waterUserContractType.getWaterSupplyContractType().getOfficeId())
                            .withName(waterUserContractType.getWaterUserContractRefType().getContractName())
                            .build())
                    .withContractExpirationDate(waterUserContractType.getContractExpirationDate())
                    .withContractedStorage(waterUserContractType.getContractedStorage())
                    .withInitialUseAllocation(waterUserContractType.getInitialUseAllocation())
                    .withFutureUseAllocation(waterUserContractType.getFutureUseAllocation())
                    .withStorageUnitsId(waterUserContractType.getStorageUnitsId())
                    .withFutureUsePercentActivated(waterUserContractType.getFutureUsePercentActivated())
                    .withTotalAllocPercentActivated(waterUserContractType.getTotalAllocPercentActivated())
                    .withPumpOutLocation(WaterSupplyUtils.map(waterUserContractType.getPumpOutLocation(),
                            PumpType.OUT))
                    .withPumpOutBelowLocation(WaterSupplyUtils.map(waterUserContractType.getPumpOutBelowLocation(),
                            PumpType.BELOW))
                    .withPumpInLocation(WaterSupplyUtils.map(waterUserContractType.getPumpInLocation(),
                            PumpType.IN))
                    .build();
            retVal.add(waterUserContract);
        }
        return retVal;
    }

    public List<LookupType> getAllWaterContractTypes(String officeId) {
        List<LookupType> retVal = new ArrayList<>();
        List<usace.cwms.db.dao.ifc.cat.LookupType> contractTypeList = connectionResult(dsl, c -> {
            setOffice(c, officeId);
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
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            WATER_USER_TAB_T waterUserObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                DSL.using(c).configuration(), projectLocationRef);
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
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            WATER_USER_TAB_T waterUserObjTs = CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                    DSL.using(c).configuration(), projectLocationRef);
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
            setOffice(c, waterContractType.getOfficeId());
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
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRefT =  LocationTypeUtil.toLocationRefT(locationRefType);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_WATER_USER(DSL.using(c).configuration(), projectLocationRefT,
                    oldWaterUser, newWaterUser);
        });
    }

    public void storeWaterUser(WaterUser waterUser, boolean failIfExists) {

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = WaterSupplyUtils
                .map(waterUser.getProjectId());

        List<WaterUserType> waterUserTypeModified = new ArrayList<>();
        waterUserTypeModified.add(new usace.cwms.db.dao.ifc.watersupply.WaterUserType(waterUser.getEntityName(),
                locationRefType, waterUser.getWaterRight()));

        connection(dsl, c -> {
            setOffice(c, waterUser.getProjectId().getOfficeId());
            WATER_USER_TAB_T waterUsers = WaterUserTypeUtil.toWaterUserTs(waterUserTypeModified);
            String pFailIfExists = OracleTypeMap.formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_WATER_USERS(DSL.using(c).configuration(), waterUsers, pFailIfExists);
        });
    }

    public void renameWaterContract(WaterUser waterUser, String oldContractName,
            String newContractName) {

        WaterUserContractRefType waterUserContractRefType =
                WaterSupplyUtils.map(waterUser, waterUser.getProjectId(), newContractName);

        connection(dsl, c -> {
            setOffice(c, waterUser.getProjectId().getOfficeId());
            WATER_USER_OBJ_T waterUserT = WaterUserTypeUtil.toWaterUserT(waterUserContractRefType.getWaterUserType());
            String contractName = waterUserContractRefType.getContractName();
            WATER_USER_CONTRACT_REF_T waterUserContract = new WATER_USER_CONTRACT_REF_T(waterUserT, contractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_CONTRACT(DSL.using(c).configuration(), waterUserContract,
                    oldContractName,
                    newContractName);
        });
    }

    public void deleteWaterUser(CwmsId location, String entityName, String deleteAction) {

        LocationRefType locationRefType = WaterSupplyUtils.map(location);

        connection(dsl, c -> {
            setOffice(c, location.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationTypeUtil.toLocationRefT(locationRefType);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_WATER_USER(DSL.using(c).configuration(), projectLocationRef,
                    entityName, deleteAction);
        });
    }

    public void deleteWaterContract(WaterUserContract contract, String deleteAction) {

        WaterUser waterUser = new WaterUser.Builder().withEntityName(contract.getWaterUser().getEntityName())
                .withProjectId(contract.getWaterUser().getProjectId())
                .withWaterRight(contract.getWaterUser().getWaterRight()).build();

        WaterUserContractRefType waterUserContractRefTypeModified =
                WaterSupplyUtils.map(waterUser, waterUser.getProjectId(), contract.getContractId().getName());

        connection(dsl, c -> {
            setOffice(c, contract.getOfficeId());
            WATER_USER_OBJ_T waterUserT = WaterUserTypeUtil.toWaterUserT(waterUserContractRefTypeModified
                    .getWaterUserType());
            String contractName = contract.getContractId().getName();
            WATER_USER_CONTRACT_REF_T waterUserContract = new WATER_USER_CONTRACT_REF_T(waterUserT, contractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_CONTRACT(DSL.using(c).configuration(), waterUserContract,
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
            setOffice(c, lookupTypes.get(0).getOfficeId());
            LOOKUP_TYPE_TAB_T contractTypes = LookupTypeUtil.buildLookupTypeTab(lookups);
            String pFailIfExists = OracleTypeMap.formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_SET_CONTRACT_TYPES(DSL.using(c).configuration(),
                    contractTypes, pFailIfExists);
        });
    }

    public void removePumpFromContract(WaterUserContract contract, String pumpLocId,
            String usageId, boolean deleteAccountingData) {

        WaterUserContractRefType contractRefType = new WaterUserContractRefType(WaterSupplyUtils
                .map(contract.getWaterUser()),
                contract.getContractId().getName());
        connection(dsl, c -> {
            setOffice(c, contract.getOfficeId());
            WATER_USER_CONTRACT_REF_T waterUserContractRefT = WaterUserTypeUtil
                    .toWaterUserContractReft(contractRefType);
            String pDeleteAccountingData = OracleTypeMap.formatBool(deleteAccountingData);
            CWMS_WATER_SUPPLY_PACKAGE.call_DISASSOCIATE_PUMP(DSL.using(c).configuration(),
                    waterUserContractRefT, pumpLocId, usageId, pDeleteAccountingData);
        });
    }
}
