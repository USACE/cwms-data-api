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
import cwms.cda.data.dto.watersupply.LocationRefType;
import cwms.cda.data.dto.watersupply.LocationType;
import cwms.cda.data.dto.watersupply.LookupType;
import cwms.cda.data.dto.watersupply.WaterSupply;
import cwms.cda.data.dto.watersupply.WaterUserContractRefType;
import cwms.cda.data.dto.watersupply.WaterUserContractType;
import cwms.cda.data.dto.watersupply.WaterUserType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.dao.CwmsDbWaterSupplyJooq;

public class WaterSupplyDao extends JooqDao<WaterSupply> {
    public WaterSupplyDao(DSLContext dsl) {
        super(dsl);
    }

    // TO DO: Get water contract number
    public List<WaterSupply> getAllWaterContracts(LocationRefType projectLocation, String entityName)
            throws SQLException {
        List<WaterSupply> retVal = new ArrayList<>();
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();
        List<usace.cwms.db.dao.ifc.watersupply.WaterUserContractType> waterUserContractTypes = new ArrayList<>();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(projectLocation);

        try {
            connection(dsl, c -> waterUserContractTypes.addAll(waterSupplyJooq.retrieveContracts(c, locationRefType,
                    entityName)));
        } catch (Exception ex) {
            throw new SQLException(ex);
        }

        for (usace.cwms.db.dao.ifc.watersupply.WaterUserContractType waterUserContractType : waterUserContractTypes) {
            WaterSupply waterSupply = new WaterSupply.Builder()
                    .withContractName(waterUserContractType.getWaterUserContractRefType().getContractName())
                    .withWaterUser(waterUserContractType.getWaterUserContractRefType()
                            .getWaterUserType().getEntityName())
                    .withUserType(map(waterUserContractType.getWaterUserContractRefType().getWaterUserType(),
                            projectLocation))
                    .withContractType(map(waterUserContractType, projectLocation))
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

    public List<WaterUserType> getAllWaterUsers(LocationRefType projectLocation) throws SQLException {
        List<WaterUserType> retVal = new ArrayList<>();
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(projectLocation);

        try {
            connection(dsl, c -> {
                List<usace.cwms.db.dao.ifc.watersupply.WaterUserType> waterUserTypes =
                        waterSupplyJooq.retrieveWaterUsers(c, locationRefType);
                for (usace.cwms.db.dao.ifc.watersupply.WaterUserType waterUserType : waterUserTypes) {
                    WaterUserType waterUser = map(waterUserType, projectLocation);
                    retVal.add(waterUser);
                }
            });
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        return retVal;
    }

    public WaterUserType getWaterUser(LocationRefType projectLocation, String entityName) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<usace.cwms.db.dao.ifc.watersupply.WaterUserType> userList = new ArrayList<>();

        connection(dsl, c -> userList.addAll(waterSupplyJooq.retrieveWaterUsers(c, map(projectLocation))));

        for (usace.cwms.db.dao.ifc.watersupply.WaterUserType waterUser : userList) {
            if (waterUser.getEntityName().equals(entityName)) {
                return map(waterUser, projectLocation);
            }
        }
        return null;
    }

    public void storeWaterContract(WaterUserContractType waterContractType, boolean failIfExists, boolean ignoreNulls) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<usace.cwms.db.dao.ifc.watersupply.WaterUserContractType> waterUserContractTypeModified = new ArrayList<>();
        waterUserContractTypeModified.add(map(waterContractType, waterContractType.getWaterUserContractRefType(),
                waterContractType.getWaterUserContractRefType().getWaterUserType().getParentLocationRefType()));
        connection(dsl, c -> waterSupplyJooq.storeContracts(c, waterUserContractTypeModified,
                failIfExists, ignoreNulls));
    }

    public void renameWaterUser(String oldWaterUser, String newWaterUser, LocationRefType projectLocation) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(projectLocation);

        connection(dsl, c -> waterSupplyJooq.renameWaterUser(c, locationRefType, oldWaterUser, newWaterUser));
    }

    public void storeWaterUser(WaterUserType waterUserType) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(waterUserType.getParentLocationRefType());

        List<usace.cwms.db.dao.ifc.watersupply.WaterUserType> waterUserTypeModified = new ArrayList<>();
        waterUserTypeModified.add(new usace.cwms.db.dao.ifc.watersupply.WaterUserType(waterUserType.getEntityName(),
                locationRefType, waterUserType.getWaterRight()));

        connection(dsl, c -> waterSupplyJooq.storeWaterUsers(c, waterUserTypeModified, true));
    }

    public void renameWaterContract(WaterUserContractRefType waterContractRefType, String oldContractName,
            String newContractName) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType waterUserContractRefType =
                map(waterContractRefType, waterContractRefType.getWaterUserType().getParentLocationRefType());

        connection(dsl, c -> waterSupplyJooq.renameContract(c, waterUserContractRefType,
                oldContractName, newContractName));
    }

    public void deleteWaterUser(LocationRefType location, String entityName, String deleteAction) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.loc.LocationRefType locationRefType = map(location);

        connection(dsl, c -> waterSupplyJooq.deleteWaterUser(c, locationRefType, entityName, deleteAction));
    }

    public void deleteWaterContract(WaterUserContractRefType waterUserContractRefType, String deleteAction) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType waterUserContractRefTypeModified =
                map(waterUserContractRefType, waterUserContractRefType.getWaterUserType().getParentLocationRefType());

        connection(dsl, c -> waterSupplyJooq.deleteContract(c, waterUserContractRefTypeModified, deleteAction));
    }

    public void storeWaterContractTypes(WaterUserContractType waterUserContractType, boolean failIfExists) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        List<usace.cwms.db.dao.ifc.watersupply.WaterUserContractType> waterUserContractTypeModified = new ArrayList<>();
        waterUserContractTypeModified.add(map(waterUserContractType,
                waterUserContractType.getWaterUserContractRefType(),
                waterUserContractType.getWaterUserContractRefType().getWaterUserType().getParentLocationRefType()));
        connection(dsl, c -> waterSupplyJooq.storeContracts(c, waterUserContractTypeModified, failIfExists));
    }

    public void removePumpFromContract(WaterUserContractRefType waterUserContractRefType, String pumpLocId,
            String usageId, boolean deleteAccounting) {
        CwmsDbWaterSupplyJooq waterSupplyJooq = new CwmsDbWaterSupplyJooq();

        usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType contractRefType =
                map(waterUserContractRefType, waterUserContractRefType.getWaterUserType().getParentLocationRefType());
        connection(dsl, c -> waterSupplyJooq.disassociatePump(c, contractRefType, pumpLocId,
                usageId, deleteAccounting));
    }

    private WaterUserType map(usace.cwms.db.dao.ifc.watersupply.WaterUserType waterUserType,
            LocationRefType projectLocation) {
        return new WaterUserType(waterUserType.getEntityName(),
                projectLocation, waterUserType.getWaterRight());
    }

    private usace.cwms.db.dao.ifc.watersupply.WaterUserType map(WaterUserType waterUserType,
            LocationRefType projectLocation) {
        return new usace.cwms.db.dao.ifc.watersupply.WaterUserType(waterUserType.getEntityName(),
                map(projectLocation), waterUserType.getWaterRight());
    }

    private LookupType map(usace.cwms.db.dao.ifc.cat.LookupType lookupType) {
        return new LookupType(lookupType.getOfficeId(),
                lookupType.getDisplayValue(), lookupType.getTooltip(), lookupType.getActive());
    }

    private usace.cwms.db.dao.ifc.cat.LookupType map(LookupType lookupType) {
        return new usace.cwms.db.dao.ifc.cat.LookupType(lookupType.getOfficeId(),
                lookupType.getDisplayValue(), lookupType.getTooltip(), lookupType.getActive());
    }

    private usace.cwms.db.dao.ifc.loc.LocationRefType map(LocationRefType projectLocation) {
        return new usace.cwms.db.dao.ifc.loc.LocationRefType(projectLocation.getBaseLocationId(),
                        projectLocation.getSubLocationId(), projectLocation.getOfficeId());
    }

    private LocationRefType map(usace.cwms.db.dao.ifc.loc.LocationRefType projectLocation) {
        return new LocationRefType(projectLocation.getBaseLocationId(), projectLocation.getSubLocationId(),
                projectLocation.getOfficeId());
    }

    private usace.cwms.db.dao.ifc.loc.LocationType map(LocationType locationType) {
        return new usace.cwms.db.dao.ifc.loc.LocationType(map(locationType.getLocationRefType()),
                        locationType.getStateInitial(), locationType.getCountyName(),
                        locationType.getTimeZoneName(), locationType.getTypeOfLocation(),
                        locationType.getLatitude(), locationType.getLongitude(),
                        locationType.getHorizontalDatum(), locationType.getElevation(),
                        locationType.getElevUnitId(), locationType.getVerticalDatum(),
                        locationType.getPublicName(), locationType.getLongName(),
                        locationType.getDescription(), locationType.getActiveFlag(),
                        locationType.getLocationKindId(), locationType.getMapLabel(),
                        locationType.getPublishedLatitude(), locationType.getPublishedLongitude(),
                        locationType.getBoundingOfficeId(), locationType.getBoundingOfficeName(),
                        locationType.getNationId(), locationType.getNearestCity());
    }

    private LocationType map(usace.cwms.db.dao.ifc.loc.LocationType locationType) {
        return new LocationType.Builder()
                .withBoundingOfficeName(locationType.getBoundingOfficeName())
                .withCountyName(locationType.getCountyName())
                .withDescription(locationType.getDescription())
                .withBoundingOfficeId(locationType.getBoundingOfficeId())
                .withActiveFlag(locationType.getActiveFlag())
                .withStateInitial(locationType.getStateInitial())
                .withElevUnitId(locationType.getElevUnitId())
                .withLatitude(locationType.getLatitude())
                .withHorizontalDatum(locationType.getHorizontalDatum())
                .withLocationKindId(locationType.getLocationKindId())
                .withMapLabel(locationType.getMapLabel())
                .withLongitude(locationType.getLongitude())
                .withLongName(locationType.getLongName())
                .withPublishedLatitude(locationType.getPublishedLatitude())
                .withPublishedLongitude(locationType.getPublishedLongitude())
                .withPublicName(locationType.getPublicName())
                .withElevation(locationType.getElevation())
                .withNearestCity(locationType.getNearestCity())
                .withNationId(locationType.getNationId())
                .withVerticalDatum(locationType.getVerticalDatum())
                .withTimeZoneName(locationType.getTimeZoneName())
                .withTypeOfLocation(locationType.getLocationType())
                .withLocationRefType(map(locationType.getLocationRef()))
                .build();
    }

    private usace.cwms.db.dao.ifc.watersupply.WaterUserContractType map(WaterUserContractType contract,
            WaterUserContractRefType waterUserContractRefType, LocationRefType projectLocation) {
        return new usace.cwms.db.dao.ifc.watersupply.WaterUserContractType(map(waterUserContractRefType,
                projectLocation),
                map(contract.getWaterSupplyContractType()), contract.getContractEffectiveDate(),
                contract.getContractExpirationDate(), contract.getContractedStorage(),
                contract.getInitialUseAllocation(), contract.getFutureUseAllocation(),
                contract.getStorageUnitsId(), contract.getFutureUsePercentActivated(),
                contract.getTotalAllocPercentActivated(), map(contract.getPumpOutLocation()),
                map(contract.getPumpOutBelowLocation()), map(contract.getPumpInLocation()));
    }

    private WaterUserContractType map(usace.cwms.db.dao.ifc.watersupply.WaterUserContractType contract,
            LocationRefType projectLocation) {
        return new WaterUserContractType.Builder()
                .withWaterSupplyContractType(map(contract.getWaterSupplyContractType()))
                .withWaterUserContractRefType(map(contract.getWaterUserContractRefType(), projectLocation))
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

    private usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType map(WaterUserContractRefType contract,
            LocationRefType projectLocation) {
        return new usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType(map(contract.getWaterUserType(),
                projectLocation), contract.getContractName());
    }

    private WaterUserContractRefType map(usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType contract,
            LocationRefType projectLocation) {
        return new WaterUserContractRefType(map(contract.getWaterUserType(), projectLocation),
                contract.getContractName());
    }
}
