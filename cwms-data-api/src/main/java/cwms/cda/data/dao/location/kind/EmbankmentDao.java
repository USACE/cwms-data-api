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
import cwms.cda.data.dto.location.kind.Embankment;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_EMBANK_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.EMBANKMENT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;

import java.util.List;

import static cwms.cda.data.dao.location.kind.LocationUtil.*;
import static java.util.stream.Collectors.toList;

public class EmbankmentDao extends JooqDao<Embankment> {

    public EmbankmentDao(DSLContext dsl) {
        super(dsl);
    }

    public List<Embankment> retrieveEmbankments(String projectLocationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(projectLocationId, officeId);
            return CWMS_EMBANK_PACKAGE.call_RETRIEVE_EMBANKMENTS(DSL.using(conn).configuration(), locationRefT)
                    .stream()
                    .map(EmbankmentDao::map)
                    .collect(toList());
        });
    }

    public Embankment retrieveEmbankment(String locationId, String officeId) {
        return connectionResult(dsl, conn -> {
            LOCATION_REF_T locationRefT = getLocationRef(locationId, officeId);
            Configuration configuration = DSL.using(conn).configuration();
            EMBANKMENT_OBJ_T embankmentObjT = CWMS_EMBANK_PACKAGE.call_RETRIEVE_EMBANKMENT(configuration, locationRefT);
            if (embankmentObjT == null) {
                throw new NotFoundException("Embankment: " + officeId + "." + locationId + " not found");
            }
            return map(embankmentObjT);
        });
    }

    static Embankment map(EMBANKMENT_OBJ_T embankment) {
        return new Embankment.Builder()
                .withStructureLength(embankment.getSTRUCTURE_LENGTH())
                .withTopWidth(embankment.getTOP_WIDTH())
                .withLengthUnits(embankment.getUNITS_ID())
                .withUpstreamSideSlope(embankment.getUPSTREAM_SIDESLOPE())
                .withDownstreamSideSlope(embankment.getDOWNSTREAM_SIDESLOPE())
                .withProjectId(getLocationIdentifier(embankment.getPROJECT_LOCATION_REF()))
                .withUpstreamProtectionType(getLookupType(embankment.getUPSTREAM_PROT_TYPE()))
                .withDownstreamProtectionType(getLookupType(embankment.getDOWNSTREAM_PROT_TYPE()))
                .withLocation(getLocation(embankment.getEMBANKMENT_LOCATION()))
                .withMaxHeight(embankment.getHEIGHT_MAX())
                .withStructureType(getLookupType(embankment.getSTRUCTURE_TYPE()))
                .build();
    }

    static EMBANKMENT_OBJ_T map(Embankment embankment) {
        EMBANKMENT_OBJ_T retval = new EMBANKMENT_OBJ_T();
        retval.setSTRUCTURE_LENGTH(embankment.getStructureLength());
        retval.setTOP_WIDTH(embankment.getTopWidth());
        retval.setUNITS_ID(embankment.getLengthUnits());
        retval.setUPSTREAM_SIDESLOPE(embankment.getUpstreamSideSlope());
        retval.setDOWNSTREAM_SIDESLOPE(embankment.getDownstreamSideSlope());
        retval.setPROJECT_LOCATION_REF(getLocationRef(embankment.getProjectId()));
        retval.setUPSTREAM_PROT_TYPE(getLookupType(embankment.getUpstreamProtectionType()));
        retval.setDOWNSTREAM_PROT_TYPE(getLookupType(embankment.getDownstreamProtectionType()));
        retval.setEMBANKMENT_LOCATION(getLocation(embankment.getLocation()));
        retval.setHEIGHT_MAX(embankment.getMaxHeight());
        retval.setSTRUCTURE_TYPE(getLookupType(embankment.getStructureType()));
        return retval;
    }

    public void storeEmbankment(Embankment embankment, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, embankment.getLocation().getOfficeId());
            CWMS_EMBANK_PACKAGE.call_STORE_EMBANKMENT(DSL.using(conn).configuration(), map(embankment), 
                    OracleTypeMap.formatBool(failIfExists));
        });
    }

    public void deleteEmbankment(String locationId, String officeId, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_EMBANK_PACKAGE.call_DELETE_EMBANKMENT(DSL.using(conn).configuration(), locationId,
                    deleteRule.getRule(), officeId);
        });
    }

    public void renameEmbankment(String officeId, String oldId, String newId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_EMBANK_PACKAGE.call_RENAME_EMBANKMENT(DSL.using(conn).configuration(), oldId,
                    newId, officeId);
        });
    }
}
