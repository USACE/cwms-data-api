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

package cwms.cda.data.dao;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.LookupType;
import static java.util.stream.Collectors.toList;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.List;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_TAB_T;

public final class LookupTypeDao extends JooqDao<LookupType> {

    public LookupTypeDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieve a list of lookup types
     * @param category - the category of the lookup type
     * @param prefix - the prefix of the lookup type
     * @param officeId - the office id
     * @return a list of lookup types
     */
    public List<LookupType> retrieveLookupTypes(String category, String prefix, String officeId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            LOOKUP_TYPE_TAB_T lookupTypes = CWMS_CAT_PACKAGE.call_GET_LOOKUP_TABLE(
                    DSL.using(conn).configuration(), category, prefix, officeId);
            return lookupTypes.stream()
                    .map(this::fromJooqLookupType)
                    .collect(toList());
        });
    }

    /**
     * Store a lookup type
     * @param category - the category of the lookup type
     * @param prefix - the prefix of the lookup type
     * @param lookupType - the lookup type to store
     */
    public void storeLookupType(String category, String prefix, LookupType lookupType) {
        connectionResult(dsl, conn -> {
            setOffice(conn, lookupType.getOfficeId());
            LOOKUP_TYPE_OBJ_T lookupTypeT = toJooqLookupType(lookupType);
            CWMS_CAT_PACKAGE.call_SET_LOOKUP_TABLE(DSL.using(conn).configuration(), new LOOKUP_TYPE_TAB_T(lookupTypeT),
                    category, prefix);
            return null;
        });

    }

    /**
     * Update a lookup type
     * @param category - the category of the lookup type
     * @param prefix - the prefix of the lookup type
     * @param lookupType - the lookup type to update
     */
    public void updateLookupType(String category, String prefix, LookupType lookupType) {
        List<LookupType> lookupTypes = retrieveLookupTypes(category, prefix, lookupType.getOfficeId());
        if (lookupTypes.isEmpty() || lookupTypes.stream().noneMatch(lt -> lt.getDisplayValue().equals(lookupType.getDisplayValue()))) {
            throw new NotFoundException("Could not find lookup type to update.");
        }
        storeLookupType(category, prefix, lookupType);
    }

    /**
     * Delete a lookup type
     * @param category - the category of the lookup type
     * @param prefix - the prefix of the lookup type
     * @param lookupType - the lookup type to delete
     */
    public void deleteLookupType(String category, String prefix, LookupType lookupType) {
        connectionResult(dsl, conn -> {
            setOffice(conn, lookupType.getOfficeId());
            LOOKUP_TYPE_OBJ_T lookupTypeT = toJooqLookupType(lookupType);
            CWMS_CAT_PACKAGE.call_DELETE_LOOKUPS(DSL.using(conn).configuration(), new LOOKUP_TYPE_TAB_T(lookupTypeT), category, prefix);
            return null;
        });
    }

    private LookupType fromJooqLookupType(LOOKUP_TYPE_OBJ_T lookupType) {
        LookupType retVal = null;
        if(lookupType != null) {
            retVal = new LookupType.Builder()
                    .withOfficeId(lookupType.getOFFICE_ID())
                    .withDisplayValue(lookupType.getDISPLAY_VALUE())
                    .withTooltip(lookupType.getTOOLTIP())
                    .withActive(OracleTypeMap.parseBool(lookupType.getACTIVE()))
                    .build();
        }
        return retVal;
    }

    private LOOKUP_TYPE_OBJ_T toJooqLookupType(LookupType lookupType) {
        LOOKUP_TYPE_OBJ_T retVal = null;
        if(lookupType != null) {
            String officeId = lookupType.getOfficeId();
            String displayValue = lookupType.getDisplayValue();
            String tooltip = lookupType.getTooltip();
            String active = OracleTypeMap.formatBool(lookupType.getActive());
            retVal = new LOOKUP_TYPE_OBJ_T(officeId, displayValue,
                    tooltip, active);
        }
        return retVal;
    }
}