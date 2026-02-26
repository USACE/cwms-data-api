/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to do so, subject to the
 * following conditions:
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

import cwms.cda.api.errors.AlreadyExists;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.formatters.xml.XMLv1;
import org.jooq.DSLContext;
import org.jooq.Record1;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_VERT_DATUM_OFFSET;

/**
 * DAO responsible for CRUD operations on Vertical Datum Info for a Location.
 */
public final class VerticalDatumDao extends JooqDao<VerticalDatumInfo> {

    public VerticalDatumDao(DSLContext dsl) {
        super(dsl);
    }

    public VerticalDatumInfo retrieveVerticalDatumInfo(String officeId, String locationId, String unit) {
        return connectionResult(dsl, conn -> {
            DSLContext ctx = getDslContext(conn, officeId);
            //using jooq to check if exists, because the package get was adding to view if it didn't exist
            verifyVerticalDatumInfoExists(ctx, officeId, locationId);
            String xml = CWMS_LOC_PACKAGE.call_GET_VERTICAL_DATUM_INFO_F__2(ctx.configuration(), locationId, unit, officeId);
            return TimeSeriesDaoImpl.parseVerticalDatumInfo(xml);
        });
    }

    public void createVerticalDatumInfo(String officeId, String locationId, VerticalDatumInfo vdi) {
        connection(dsl, conn -> {
            DSLContext ctx = getDslContext(conn, officeId);
            verifyVerticalDatumInfoDoesNotExist(ctx, officeId, locationId);
            store(ctx, officeId, locationId, vdi);
        });
    }

    public void updateVerticalDatumInfo(String officeId, String locationId, VerticalDatumInfo vdi) {
        connection(dsl, conn -> {
            DSLContext ctx = getDslContext(conn, officeId);
            verifyVerticalDatumInfoExists(ctx, officeId, locationId);
            store(ctx, officeId, locationId, vdi);
        });
    }

    private void store(DSLContext ctx, String officeId, String locationId, VerticalDatumInfo vdi) {
        //we pass in the location and office, and including them in the xml causes store issues in the db so, removing since they aren't necessary
        VerticalDatumInfo vdiWithoutLocAndOffice = new VerticalDatumInfo.Builder().from(vdi)
                .withOffice(null)
                .withLocation(null)
                .build();
        String xml = new XMLv1().format(vdiWithoutLocAndOffice);
        CWMS_LOC_PACKAGE.call_SET_VERTICAL_DATUM_INFO__3(ctx.configuration(), locationId, xml, formatBool(false), officeId);
    }


    public void deleteVerticalDatumInfo(String officeId, String locationId) {
        connection(dsl, conn -> {
            DSLContext ctx = getDslContext(conn, officeId);
            verifyVerticalDatumInfoExists(ctx, officeId, locationId);
            VerticalDatumInfo emptyVdi = new VerticalDatumInfo.Builder()
                    .withLocation(locationId)
                    .withOffice(officeId)
                    .withUnit("m")
                    .build();
            String emptyXml = new XMLv1().format(emptyVdi);
            CWMS_LOC_PACKAGE.call_SET_VERTICAL_DATUM_INFO(ctx.configuration(), emptyXml, formatBool(false));
            CWMS_LOC_PACKAGE.call_DELETE_LOCAL_VERT_DATUM_NAME__2(ctx.configuration(), locationId, officeId);
        });
    }

    private void verifyVerticalDatumInfoExists(DSLContext ctx, String officeId, String locationId) {
        Record1<usace.cwms.db.jooq.codegen.tables.records.AV_VERT_DATUM_OFFSET> result = ctx.select(AV_VERT_DATUM_OFFSET.AV_VERT_DATUM_OFFSET).from(AV_VERT_DATUM_OFFSET.AV_VERT_DATUM_OFFSET)
                .where(AV_VERT_DATUM_OFFSET.AV_VERT_DATUM_OFFSET.LOCATION_ID.eq(locationId))
                .and(AV_VERT_DATUM_OFFSET.AV_VERT_DATUM_OFFSET.OFFICE_ID.eq(officeId))
                .fetchOne();
        if(result == null) {
            throw new NotFoundException("No vertical datum info found for location " + locationId + " in office " + officeId);
        }
    }

    private void verifyVerticalDatumInfoDoesNotExist(DSLContext ctx, String officeId, String locationId) {
        try {
            verifyVerticalDatumInfoExists(ctx, officeId, locationId);
        } catch (NotFoundException e) {
            return;
        }
        throw new AlreadyExists("Vertical datum info already exists for location " + locationId + " in office " + officeId, null);
    }
}
