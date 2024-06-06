/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;

import java.time.ZoneId;
import java.util.Optional;

final class LocationUtil {

    private LocationUtil() {
        throw new AssertionError("Utility class");
    }

    static String getLocationId(LOCATION_REF_T ref) {
        String locationId = null;
        if(ref != null) {
            locationId = ref.getBASE_LOCATION_ID();
            String sub = ref.getSUB_LOCATION_ID();
            if (sub != null && !sub.isEmpty()) {
                locationId += "-" + sub;
            }
        }
        return locationId;
    }

    static LOCATION_REF_T getLocationRef(String locationId, String officeId) {
        LOCATION_REF_T retval = null;
        if(locationId != null && !locationId.isEmpty()) {
            retval = new LOCATION_REF_T();
            String[] split = locationId.split("-");
            retval.setBASE_LOCATION_ID(split[0]);
            if(split.length > 1) {
                retval.setSUB_LOCATION_ID(split[1]);
            }
            retval.setOFFICE_ID(officeId);
        }
        return retval;
    }

    static LookupType getLookupType(LOOKUP_TYPE_OBJ_T lookupType) {
        LookupType retval = null;
        if(lookupType != null) {
            retval = new LookupType.Builder()
                    .withOfficeId(lookupType.getOFFICE_ID())
                    .withActive(OracleTypeMap.parseBool(lookupType.getACTIVE()))
                    .withDisplayValue(lookupType.getDISPLAY_VALUE())
                    .withTooltip(lookupType.getTOOLTIP())
                    .build();
        }
        return retval;
    }

    static LOOKUP_TYPE_OBJ_T getLookupType(LookupType lookupType) {
        LOOKUP_TYPE_OBJ_T retval = null;
        if(lookupType != null) {
            retval = new LOOKUP_TYPE_OBJ_T();
            retval.setOFFICE_ID(lookupType.getOfficeId());
            retval.setACTIVE(OracleTypeMap.formatBool(lookupType.getActive()));
            retval.setDISPLAY_VALUE(lookupType.getDisplayValue());
            retval.setTOOLTIP(lookupType.getTooltip());
        }
        return retval;
    }

    static Location getLocation(LOCATION_OBJ_T location) {
        Location retval = null;
        if(location != null) {
            retval = new Location.Builder(getLocationId(location.getLOCATION_REF()),
                    location.getLOCATION_KIND_ID(),
                    ZoneId.of(location.getTIME_ZONE_NAME()),
                    OracleTypeMap.buildDouble(location.getLATITUDE()),
                    OracleTypeMap.buildDouble(location.getLONGITUDE()),
                    location.getHORIZONTAL_DATUM(),
                    location.getLOCATION_REF().getOFFICE_ID())
                    .withActive(OracleTypeMap.parseBool(location.getACTIVE_FLAG()))
                    .withDescription(location.getDESCRIPTION())
                    .withElevation(OracleTypeMap.buildDouble(location.getELEVATION()))
                    .withCountyName(location.getCOUNTY_NAME())
                    .withBoundingOfficeId(location.getBOUNDING_OFFICE_ID())
                    .withNation(Nation.nationForName(location.getNATION_ID()))
                    .withMapLabel(location.getMAP_LABEL())
                    .withPublicName(location.getPUBLIC_NAME())
                    .withPublishedLatitude(OracleTypeMap.buildDouble(location.getPUBLISHED_LATITUDE()))
                    .withPublishedLongitude(OracleTypeMap.buildDouble(location.getPUBLISHED_LONGITUDE()))
                    .withVerticalDatum(location.getVERTICAL_DATUM())
                    .withLongName(location.getLONG_NAME())
                    .withStateInitial(location.getSTATE_INITIAL())
                    .withLocationType(location.getLOCATION_TYPE())
                    .build();
        }
        return retval;
    }

    static LOCATION_OBJ_T getLocation(Location location) {
        LOCATION_OBJ_T retval = null;
        if(location != null) {
            retval = new LOCATION_OBJ_T();
            retval.setLOCATION_REF(getLocationRef(location.getName(), location.getOfficeId()));
            retval.setLOCATION_KIND_ID(location.getLocationKind());
            retval.setTIME_ZONE_NAME(location.getTimezoneName());
            retval.setLATITUDE(OracleTypeMap.toBigDecimal(location.getLatitude()));
            retval.setLONGITUDE(OracleTypeMap.toBigDecimal(location.getLongitude()));
            retval.setHORIZONTAL_DATUM(location.getHorizontalDatum());
            retval.setACTIVE_FLAG(OracleTypeMap.formatBool(location.getActive()));
            retval.setDESCRIPTION(location.getDescription());
            retval.setELEVATION(OracleTypeMap.toBigDecimal(location.getElevation()));
            retval.setELEV_UNIT_ID("m");
            retval.setCOUNTY_NAME(location.getCountyName());
            retval.setBOUNDING_OFFICE_ID(location.getBoundingOfficeId());
            retval.setNATION_ID(Optional.ofNullable(location.getNation()).map(Nation::getName).orElse(null));
            retval.setMAP_LABEL(location.getMapLabel());
            retval.setPUBLIC_NAME(location.getPublicName());
            retval.setPUBLISHED_LATITUDE(OracleTypeMap.toBigDecimal(location.getPublishedLatitude()));
            retval.setPUBLISHED_LONGITUDE(OracleTypeMap.toBigDecimal(location.getPublishedLongitude()));
            retval.setVERTICAL_DATUM(location.getVerticalDatum());
            retval.setLONG_NAME(location.getLongName());
            retval.setSTATE_INITIAL(location.getStateInitial());
            retval.setLOCATION_TYPE(location.getLocationType());
        }
        return retval;
    }
}
