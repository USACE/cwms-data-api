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

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.WaterSupply;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.data.dto.watersupply.WaterUserContractRef;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.loc.LocationType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserType;
import usace.cwms.db.jooq.dao.CwmsDbWaterSupplyJooq;

public class WaterSupplyDao extends JooqDao<WaterSupply> {
    public WaterSupplyDao(DSLContext dsl) {
        super(dsl);
    }

    // TO DO: Get water contract number
    public List<WaterSupply> getAllWaterContracts(CwmsId projectLocation, String entityName)
            throws SQLException {
        List<WaterSupply> retVal = new ArrayList<>();
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();
        List<WaterUserContractType> waterUserContractTypes = new ArrayList<>();

        LocationRefType locationRefType = map(projectLocation);

        try {
            connection(dsl, c -> waterUserContractTypes.addAll(waterSupplyJooq.retrieveContracts(c, locationRefType,
                    entityName)));
        } catch (Exception ex) {
            throw new SQLException(ex);
        }

        for (WaterUserContractType waterUserContractType : waterUserContractTypes) {
            WaterSupply waterSupply = new WaterSupply.Builder()
                    .withContractName(waterUserContractType.getWaterUserContractRefType().getContractName())
                    .withWaterUser(waterUserContractType.getWaterUserContractRefType()
                            .getWaterUserType().getEntityName())
                    .withUser(map(waterUserContractType.getWaterUserContractRefType().getWaterUserType(),
                            projectLocation))
                    .withContract(map(waterUserContractType, projectLocation))
                    .build();
            retVal.add(waterSupply);
        }

        return retVal;
    }

    public List<LookupType> getAllWaterContractTypes(String officeId) throws SQLException {
        List<LookupType> retVal = new ArrayList<>();
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();
        try {
            connection(dsl, c -> {
                List<usace.cwms.db.dao.ifc.cat.LookupType> contractTypeList =
                        waterSupplyJooq.getContractTypes(c, officeId);
                for (usace.cwms.db.dao.ifc.cat.LookupType lookupType : contractTypeList) {
                    LookupType waterUserContractType = map(lookupType);
                    retVal.add(waterUserContractType);
                }
            });
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return retVal;
    }

    public List<WaterUser> getAllWaterUsers(CwmsId projectLocation) throws SQLException {
        List<WaterUser> retVal = new ArrayList<>();
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        LocationRefType locationRefType = map(projectLocation);

        try {
            connection(dsl, c -> {
                List<WaterUserType> waterUserTypes =
                        waterSupplyJooq.retrieveWaterUsers(c, locationRefType);
                for (WaterUserType waterUserType : waterUserTypes) {
                    WaterUser waterUser = map(waterUserType, projectLocation);
                    retVal.add(waterUser);
                }
            });
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return retVal;
    }

    public WaterUser getWaterUser(CwmsId projectLocation, String entityName) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<WaterUserType> userList = new ArrayList<>();

        connection(dsl, c -> userList.addAll(waterSupplyJooq.retrieveWaterUsers(c, map(projectLocation))));

        for (WaterUserType waterUser : userList) {
            if (waterUser.getEntityName().equals(entityName)) {
                return map(waterUser, projectLocation);
            }
        }
        return null;
    }

    public void storeWaterContract(WaterUserContract waterContractType, boolean failIfExists, boolean ignoreNulls) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<WaterUserContractType> waterUserContractTypeModified = new ArrayList<>();
        waterUserContractTypeModified.add(map(waterContractType, waterContractType.getWaterUserContractRef(),
                waterContractType.getWaterUserContractRef().getWaterUser().getParentLocationRef()));
        connection(dsl, c -> waterSupplyJooq.storeContracts(c, waterUserContractTypeModified,
                failIfExists, ignoreNulls));
    }

    public void renameWaterUser(String oldWaterUser, String newWaterUser, CwmsId projectLocation) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        LocationRefType locationRefType = map(projectLocation);

        connection(dsl, c -> waterSupplyJooq.renameWaterUser(c, locationRefType, oldWaterUser, newWaterUser));
    }

    public void storeWaterUser(WaterUser waterUserType) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(waterUserType.getParentLocationRef());

        List<WaterUserType> waterUserTypeModified = new ArrayList<>();
        waterUserTypeModified.add(new usace.cwms.db.dao.ifc.watersupply.WaterUserType(waterUserType.getEntityName(),
                locationRefType, waterUserType.getWaterRight()));

        connection(dsl, c -> waterSupplyJooq.storeWaterUsers(c, waterUserTypeModified, true));
    }

    public void renameWaterContract(WaterUserContractRef waterContractRefType, String oldContractName,
            String newContractName) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        WaterUserContractRefType waterUserContractRefType =
                map(waterContractRefType, waterContractRefType.getWaterUser().getParentLocationRef());

        connection(dsl, c -> waterSupplyJooq.renameContract(c, waterUserContractRefType,
                oldContractName, newContractName));
    }

    public void deleteWaterUser(CwmsId location, String entityName, String deleteAction) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        LocationRefType locationRefType = map(location);

        connection(dsl, c -> waterSupplyJooq.deleteWaterUser(c, locationRefType, entityName, deleteAction));
    }

    public void deleteWaterContract(WaterUserContractRef waterUserContractRefType, String deleteAction) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        WaterUserContractRefType waterUserContractRefTypeModified =
                map(waterUserContractRefType, waterUserContractRefType.getWaterUser().getParentLocationRef());

        connection(dsl, c -> waterSupplyJooq.deleteContract(c, waterUserContractRefTypeModified, deleteAction));
    }

    public void storeWaterContractTypes(WaterUserContract waterUserContractType, boolean failIfExists) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<WaterUserContractType> waterUserContractTypeModified = new ArrayList<>();
        waterUserContractTypeModified.add(map(waterUserContractType,
                waterUserContractType.getWaterUserContractRef(),
                waterUserContractType.getWaterUserContractRef().getWaterUser().getParentLocationRef()));
        connection(dsl, c -> waterSupplyJooq.storeContracts(c, waterUserContractTypeModified, failIfExists));
    }

    public void removePumpFromContract(WaterUserContractRef waterUserContractRefType, String pumpLocId,
            String usageId, boolean deleteAccounting) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        WaterUserContractRefType contractRefType =
                map(waterUserContractRefType, waterUserContractRefType.getWaterUser().getParentLocationRef());
        connection(dsl, c -> waterSupplyJooq.disassociatePump(c, contractRefType, pumpLocId,
                usageId, deleteAccounting));
    }

    private WaterUser map(WaterUserType waterUserType,
            CwmsId projectLocation) {
        return new WaterUser(waterUserType.getEntityName(),
                projectLocation, waterUserType.getWaterRight());
    }

    private WaterUserType map(WaterUser waterUserType,
            CwmsId projectLocation) {
        return new WaterUserType(waterUserType.getEntityName(),
                map(projectLocation), waterUserType.getWaterRight());
    }

    private LookupType map(usace.cwms.db.dao.ifc.cat.LookupType lookupType) {
        return new LookupType.Builder()
                .withOfficeId(lookupType.getOfficeId())
                .withDisplayValue(lookupType.getDisplayValue())
                .withTooltip(lookupType.getTooltip())
                .withActive(lookupType.getActive())
                .build();
    }

    private usace.cwms.db.dao.ifc.cat.LookupType map(LookupType lookupType) {
        return new usace.cwms.db.dao.ifc.cat.LookupType(lookupType.getOfficeId(),
                lookupType.getDisplayValue(), lookupType.getTooltip(), lookupType.getActive());
    }

    private LocationRefType map(CwmsId projectLocation) {
        return new LocationRefType(projectLocation.getName(),"",
                        projectLocation.getOfficeId());
    }

    private LocationType map(Location location) {
        return new LocationType(new LocationRefType(location.getName(),
                "", location.getOfficeId()),
                        location.getStateInitial(), location.getCountyName(),
                        location.getTimezoneName(), location.getLocationType(),
                        location.getLatitude(), location.getLongitude(),
                        location.getHorizontalDatum(), location.getElevation(),
                        location.getElevationUnits(), location.getVerticalDatum(),
                        location.getPublicName(), location.getLongName(),
                        location.getDescription(), location.getActive(),
                        location.getLocationKind(), location.getMapLabel(),
                        location.getPublishedLatitude(), location.getPublishedLongitude(),
                        location.getBoundingOfficeId(), location.getBoundingOfficeId(),
                        location.getNation().toString(), location.getNearestCity());
    }

    private Location map(LocationType location) {
        return new Location.Builder(location.getBoundingOfficeId(), location.getPublicName())
                .withLocationKind(location.getLocationKindId())
                .withStateInitial(location.getStateInitial())
                .withCountyName(location.getCountyName())
                .withTimeZoneName(ZoneId.of(location.getTimeZoneName()))
                .withLatitude(location.getLatitude())
                .withLongitude(location.getLongitude())
                .withHorizontalDatum(location.getHorizontalDatum())
                .withElevation(location.getElevation())
                .withElevationUnits(location.getElevUnitId())
                .withVerticalDatum(location.getVerticalDatum())
                .withLongName(location.getLongName())
                .withDescription(location.getDescription())
                .withActive(location.getActiveFlag())
                .withMapLabel(location.getMapLabel())
                .withPublishedLatitude(location.getPublishedLatitude())
                .withPublishedLongitude(location.getPublishedLongitude())
                .withNation(Nation.valueOf(location.getNationId()))
                .withNearestCity(location.getNearestCity())
                .build();
    }

    private WaterUserContractType map(WaterUserContract contract,
            WaterUserContractRef waterUserContractRef, CwmsId projectLocation) {
        return new WaterUserContractType(map(waterUserContractRef,
                projectLocation),
                map(contract.getWaterSupplyContract()), contract.getContractEffectiveDate(),
                contract.getContractExpirationDate(), contract.getContractedStorage(),
                contract.getInitialUseAllocation(), contract.getFutureUseAllocation(),
                contract.getStorageUnitsId(), contract.getFutureUsePercentActivated(),
                contract.getTotalAllocPercentActivated(), map(contract.getPumpOutLocation()),
                map(contract.getPumpOutBelowLocation()), map(contract.getPumpInLocation()));
    }

    private WaterUserContract map(usace.cwms.db.dao.ifc.watersupply.WaterUserContractType contract,
            CwmsId projectLocation) {
        return new WaterUserContract.Builder()
                .withWaterSupplyContract(map(contract.getWaterSupplyContractType()))
                .withWaterUserContractRef(map(contract.getWaterUserContractRefType(), projectLocation))
                .withContractEffectiveDate(contract.getContractEffectiveDate())
                .withContractExpirationDate(contract.getContractExpirationDate())
                .withContractedStorage(contract.getContractedStorage())
                .withInitialUseAllocation(contract.getInitialUseAllocation())
                .withFutureUseAllocation(contract.getFutureUseAllocation())
                .withStorageUnitsId(contract.getStorageUnitsId())
                .withFutureUsePercentActivated(contract.getFutureUsePercentActivated())
                .withTotalAllocPercentActivated(contract.getTotalAllocPercentActivated())
                .withPumpOutLocation(map(contract.getPumpOutLocation()))
                .withPumpOutBelowLocation(map(contract.getPumpOutBelowLocation()))
                .withPumpInLocation(map(contract.getPumpInLocation()))
                .build();
    }

    private WaterUserContractRefType map(WaterUserContractRef contract,
            CwmsId projectLocation) {
        return new WaterUserContractRefType(map(contract.getWaterUser(),
                projectLocation), contract.getContractName());
    }

    private WaterUserContractRef map(WaterUserContractRefType contract,
            CwmsId projectLocation) {
        return new WaterUserContractRef(map(contract.getWaterUserType(), projectLocation),
                contract.getContractName());
    }
}
