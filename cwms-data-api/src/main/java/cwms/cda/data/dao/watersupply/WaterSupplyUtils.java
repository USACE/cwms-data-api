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
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.data.dto.watersupply.WaterUserContractRef;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.loc.LocationType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractRefType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractType;
import usace.cwms.db.dao.ifc.watersupply.WaterUserType;

public class WaterSupplyUtils {

    private WaterSupplyUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static WaterUser map(WaterUserType waterUserType,
            CwmsId projectLocation) {
        return new WaterUser(waterUserType.getEntityName(),
                projectLocation, waterUserType.getWaterRight());
    }

    public static WaterUserType map(WaterUser waterUserType,
            CwmsId projectLocation) {
        return new WaterUserType(waterUserType.getEntityName(),
                map(projectLocation), waterUserType.getWaterRight());
    }

    public static WaterUserType map(WaterUser waterUser) {
        return new WaterUserType(waterUser.getEntityName(),
                map(waterUser.getParentLocationRef()), waterUser.getWaterRight());
    }

    public static LookupType map(usace.cwms.db.dao.ifc.cat.LookupType lookupType) {
        return new LookupType.Builder()
                .withOfficeId(lookupType.getOfficeId())
                .withDisplayValue(lookupType.getDisplayValue())
                .withTooltip(lookupType.getTooltip())
                .withActive(lookupType.getActive())
                .build();
    }

    public static usace.cwms.db.dao.ifc.cat.LookupType map(LookupType lookupType) {
        return new usace.cwms.db.dao.ifc.cat.LookupType(lookupType.getOfficeId(),
                lookupType.getDisplayValue(), lookupType.getTooltip(), lookupType.getActive());
    }

    public static LocationRefType map(CwmsId projectLocation) {
        return new LocationRefType(projectLocation.getName(),"",
                projectLocation.getOfficeId());
    }

    public static WaterSupplyPump map(LocationType locationType) {
        return new WaterSupplyPump(new Location.Builder(locationType.getLocationRef().getOfficeId(),
                locationType.getLocationRef().getBaseLocationId())
                .withNearestCity(locationType.getNearestCity())
                .withNation(Nation.valueOf(locationType.getNationId()))
                .build(),
                null,
                new CwmsId.Builder()
                        .withName(locationType.getLocationRef().getBaseLocationId())
                        .withOfficeId(locationType.getLocationRef().getOfficeId())
                        .build());
    }

    public static LocationType map(WaterSupplyPump pump) {
        return new LocationType(new LocationRefType(pump.getPumpId().getName(),
                "", pump.getPumpId().getOfficeId()),
                pump.getPumpLocation().getStateInitial(),
                pump.getPumpLocation().getCountyName(),
                pump.getPumpLocation().getTimezoneName(),
                pump.getPumpLocation().getLocationKind(),
                pump.getPumpLocation().getLatitude(),
                pump.getPumpLocation().getLongitude(),
                pump.getPumpLocation().getHorizontalDatum(),
                pump.getPumpLocation().getElevation(),
                pump.getPumpLocation().getElevationUnits(),
                pump.getPumpLocation().getVerticalDatum(),
                pump.getPumpLocation().getPublicName(),
                pump.getPumpLocation().getLongName(),
                pump.getPumpLocation().getDescription(),
                pump.getPumpLocation().getActive(),
                pump.getPumpLocation().getLocationKind(),
                pump.getPumpLocation().getMapLabel(),
                pump.getPumpLocation().getPublishedLatitude(),
                pump.getPumpLocation().getPublishedLongitude(),
                pump.getPumpLocation().getBoundingOfficeId(),
                pump.getPumpLocation().getBoundingOfficeId(),
                pump.getPumpLocation().getNation().toString(),
                pump.getPumpLocation().getNearestCity());
    }

    public static WaterUserContractType map(WaterUserContract contract) {
        return new WaterUserContractType(new WaterUserContractRefType(map(contract.getWaterUser()), contract.getContractId().getName()),
                map(contract.getWaterContract()), contract.getContractEffectiveDate(),
                contract.getContractExpirationDate(), contract.getContractedStorage(),
                contract.getInitialUseAllocation(), contract.getFutureUseAllocation(),
                contract.getStorageUnitsId(), contract.getFutureUsePercentActivated(),
                contract.getTotalAllocPercentActivated(), map(contract.getPumpOutLocation()),
                map(contract.getPumpOutBelowLocation()), map(contract.getPumpInLocation()));
    }


    public static WaterUserContractRefType map(WaterUserContractRef contract,
            CwmsId projectLocation) {
        return new WaterUserContractRefType(map(contract.getWaterUser(),
                projectLocation), contract.getContractName());
    }

    public static WaterUserContractRef map(WaterUserContractRefType contract,
            CwmsId projectLocation) {
        return new WaterUserContractRef(map(contract.getWaterUserType(), projectLocation),
                contract.getContractName());
    }
}
