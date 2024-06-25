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


import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.location.kind.Turbine;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TURBINE_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_STRUCTURE_OBJ_T;

import java.util.List;

import static cwms.cda.data.dao.location.kind.LocationUtil.*;
import static java.util.stream.Collectors.toList;

public class TurbineDao extends JooqDao<Turbine> {

    public TurbineDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Turbine> retrieveTurbines(String projectLocationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(projectLocationId, officeId);
            return CWMS_TURBINE_PACKAGE.call_RETRIEVE_TURBINES(DSL.using(conn).configuration(), locationRefT)
                    .stream()
                    .map(TurbineDao::map)
                    .collect(toList());
        });
    }

    public Turbine retrieveTurbine(String locationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(locationId, officeId);
            Configuration configuration = DSL.using(conn).configuration();
            PROJECT_STRUCTURE_OBJ_T turbineObjT = CWMS_TURBINE_PACKAGE.call_RETRIEVE_TURBINE(configuration, locationRefT);
            if (turbineObjT == null) {
                throw new NotFoundException("Turbine: " + officeId + "." + locationId + " not found");
            }
            return map(turbineObjT);
        });
    }

    static Turbine map(PROJECT_STRUCTURE_OBJ_T turbine) {
        return new Turbine.Builder()
                .withProjectId(getLocationIdentifier(turbine.getPROJECT_LOCATION_REF()))
                .withLocation(getLocation(turbine.getSTRUCTURE_LOCATION()))
                .build();
    }

    static PROJECT_STRUCTURE_OBJ_T map(Turbine turbine) {
        PROJECT_STRUCTURE_OBJ_T retval = new PROJECT_STRUCTURE_OBJ_T();
        retval.setPROJECT_LOCATION_REF(getLocationRef(turbine.getProjectId()));
        retval.setSTRUCTURE_LOCATION(getLocation(turbine.getLocation()));
        return retval;
    }

    public void storeTurbine(Turbine turbine, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, turbine.getLocation().getOfficeId());
            CWMS_TURBINE_PACKAGE.call_STORE_TURBINE(DSL.using(conn).configuration(), map(turbine), 
                    OracleTypeMap.formatBool(failIfExists));
        });
    }

    public void deleteTurbine(String locationId, String officeId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TURBINE_PACKAGE.call_DELETE_TURBINE(DSL.using(conn).configuration(), locationId,
                    deleteRule.getRule(), officeId);
        });
    }

    public void renameTurbine(String officeId, String oldId, String newId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TURBINE_PACKAGE.call_RENAME_TURBINE(DSL.using(conn).configuration(), oldId,
                    newId, officeId);
        });
    }
}
