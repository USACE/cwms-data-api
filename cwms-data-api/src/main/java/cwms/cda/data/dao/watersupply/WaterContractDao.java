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

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_TAB_T;


public final class WaterContractDao extends JooqDao<WaterUserContract> {
    public WaterContractDao(DSLContext dsl) {
        super(dsl);
    }

    public List<WaterUserContract> getAllWaterContracts(CwmsId projectLocation, String entityName) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationUtil.getLocationRef(projectLocation);
            return CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_CONTRACTS(
                    DSL.using(c).configuration(), projectLocationRef, entityName)
                    .stream()
                    .map(WaterSupplyUtils::toWaterContract)
                    .collect(toList());
        });
    }

    public WaterUserContract getWaterContract(String contractName, CwmsId projectLocation, String entityName) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationUtil.getLocationRef(projectLocation);
            return CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_CONTRACTS(
                    DSL.using(c).configuration(), projectLocationRef, entityName)
                    .stream()
                    .map(WaterSupplyUtils::toWaterContract)
                    .filter(contract -> contract.getContractId().getName().equals(contractName))
                    .findAny()
                    .orElseThrow(() -> new NotFoundException("Water contract not found: " + contractName));
        });
    }

    public List<LookupType> getAllWaterContractTypes(String officeId) {
        return connectionResult(dsl, c -> {
            setOffice(c, officeId);
            return CWMS_WATER_SUPPLY_PACKAGE.call_GET_CONTRACT_TYPES(
                    DSL.using(c).configuration(), officeId)
                    .stream()
                    .map(LocationUtil::getLookupType)
                    .collect(toList());
        });
    }

    public List<WaterUser> getAllWaterUsers(CwmsId projectLocation) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationUtil.getLocationRef(projectLocation);
            return CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                DSL.using(c).configuration(), projectLocationRef)
                .stream()
                .map(WaterSupplyUtils::toWaterUser)
                .collect(toList());
        });
    }

    public WaterUser getWaterUser(CwmsId projectLocation, String entityName) {
        return connectionResult(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationUtil.getLocationRef(projectLocation);
            return CWMS_WATER_SUPPLY_PACKAGE.call_RETRIEVE_WATER_USERS(
                    DSL.using(c).configuration(), projectLocationRef)
                    .stream()
                    .map(WaterSupplyUtils::toWaterUser)
                    .filter(waterUser -> waterUser.getEntityName().equals(entityName))
                    .findAny()
                    .orElseThrow(() -> new NotFoundException("Water user not found: " + entityName));
        });
    }

    public void storeWaterContract(WaterUserContract waterContract, boolean failIfExists, boolean ignoreNulls) {
        connection(dsl, c -> {
            setOffice(c, waterContract.getOfficeId());
            String paramFailIfExists = formatBool(failIfExists);
            String paramIgnoreNulls = formatBool(ignoreNulls);
            WATER_USER_CONTRACT_TAB_T paramContracts = WaterSupplyUtils.toWaterUserContractTs(waterContract);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_CONTRACTS2(DSL.using(c).configuration(), paramContracts,
                    paramFailIfExists, paramIgnoreNulls);
        });
    }

    public void renameWaterUser(String oldWaterUser, String newWaterUser, CwmsId projectLocation) {
        connection(dsl, c -> {
            setOffice(c, projectLocation.getOfficeId());
            LOCATION_REF_T projectLocationRefT =  LocationUtil.getLocationRef(projectLocation);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_WATER_USER(DSL.using(c).configuration(), projectLocationRefT,
                    oldWaterUser, newWaterUser);
        });
    }

    public void storeWaterUser(WaterUser waterUser, boolean failIfExists) {
        connection(dsl, c -> {
            setOffice(c, waterUser.getProjectId().getOfficeId());
            WATER_USER_TAB_T waterUsers = WaterSupplyUtils.toWaterUserTs(waterUser);
            String paramFailIfExists = formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_STORE_WATER_USERS(DSL.using(c).configuration(),
                    waterUsers, paramFailIfExists);
        });
    }

    public void renameWaterContract(WaterUser waterUser, String oldContractName,
            String newContractName) {
        connection(dsl, c -> {
            setOffice(c, waterUser.getProjectId().getOfficeId());
            WATER_USER_OBJ_T waterUserT = WaterSupplyUtils.toWaterUser(waterUser);
            WATER_USER_CONTRACT_REF_T waterUserContract = new WATER_USER_CONTRACT_REF_T(waterUserT, oldContractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_RENAME_CONTRACT(DSL.using(c).configuration(), waterUserContract,
                    oldContractName, newContractName);
        });
    }

    public void deleteWaterUser(CwmsId location, String entityName, DeleteMethod deleteAction) {
        connection(dsl, c -> {
            setOffice(c, location.getOfficeId());
            LOCATION_REF_T projectLocationRef =  LocationUtil.getLocationRef(location);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_WATER_USER(DSL.using(c).configuration(), projectLocationRef,
                    entityName, deleteAction.getRule().toString());
        });
    }

    public void deleteWaterContract(WaterUserContract contract, DeleteMethod deleteAction) {
        connection(dsl, c -> {
            setOffice(c, contract.getOfficeId());
            WATER_USER_OBJ_T waterUserT = WaterSupplyUtils.toWaterUser(contract.getWaterUser());
            String contractName = contract.getContractId().getName();
            WATER_USER_CONTRACT_REF_T waterUserContract = new WATER_USER_CONTRACT_REF_T(waterUserT, contractName);
            CWMS_WATER_SUPPLY_PACKAGE.call_DELETE_CONTRACT(DSL.using(c).configuration(), waterUserContract,
                    deleteAction.getRule().toString());
        });
    }

    public void storeWaterContractTypes(LookupType lookupType,
            boolean failIfExists) {
        connection(dsl, c -> {
            setOffice(c, lookupType.getOfficeId());
            LOOKUP_TYPE_TAB_T contractTypes = WaterSupplyUtils.toLookupTypeT(lookupType);
            String paramFailIfExists = formatBool(failIfExists);
            CWMS_WATER_SUPPLY_PACKAGE.call_SET_CONTRACT_TYPES(DSL.using(c).configuration(),
                    contractTypes, paramFailIfExists);
        });
    }

    public void removePumpFromContract(WaterUserContract contract, String pumpLocName,
            PumpType pumpType, boolean deleteAccountingData) {
        connection(dsl, c -> {
            setOffice(c, contract.getOfficeId());
            WATER_USER_CONTRACT_REF_T waterUserContractRefT = WaterSupplyUtils
                    .toWaterUserContractRefTs(contract);
            String paramDeleteAccountingData = formatBool(deleteAccountingData);
            CWMS_WATER_SUPPLY_PACKAGE.call_DISASSOCIATE_PUMP(DSL.using(c).configuration(),
                    waterUserContractRefT, pumpLocName, pumpType.toString(), paramDeleteAccountingData);
        });
    }
}
