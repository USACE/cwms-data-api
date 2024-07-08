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
import cwms.cda.data.dto.Property;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROPERTIES_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_properties.GET_PROPERTY__2;
import usace.cwms.db.jooq.codegen.tables.AV_PROPERTY;
import usace.cwms.db.jooq.codegen.udt.records.PROPERTY_INFO_T;

import java.util.List;

public final class PropertyDao extends JooqDao<Property> {

    public PropertyDao(DSLContext dsl) {
        super(dsl);
    }

    public Property retrieveProperty(String office, String category, String name, String defaultValue) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, office);
            GET_PROPERTY__2 value = CWMS_PROPERTIES_PACKAGE.call_GET_PROPERTY__2(DSL.using(conn).configuration(), category, name, defaultValue, office);
            return new Property.Builder()
                    .withOfficeId(office)
                    .withCategory(category)
                    .withName(name)
                    .withValue(value.getP_VALUE())
                    .withComment(value.getP_COMMENT())
                    .build();
        });
    }

    public List<Property> retrieveProperties(String officeIdMask, String categoryMask, String idMask) {
        return connectionResult(dsl, conn -> {
            PROPERTY_INFO_T propInfo = new PROPERTY_INFO_T(officeIdMask, categoryMask, idMask);
            return CWMS_PROPERTIES_PACKAGE.call_GET_PROPERTIES__4(DSL.using(conn).configuration(), propInfo)
                    .map(r -> new Property.Builder()
                            .withOfficeId(r.get(AV_PROPERTY.AV_PROPERTY.OFFICE_ID))
                            .withCategory(r.get(AV_PROPERTY.AV_PROPERTY.PROP_CATEGORY))
                            .withName(r.get(AV_PROPERTY.AV_PROPERTY.PROP_ID))
                            .withValue(r.get(AV_PROPERTY.AV_PROPERTY.PROP_VALUE))
                            .withComment(r.get(AV_PROPERTY.AV_PROPERTY.PROP_COMMENT))
                            .build());
        });
    }

    public void updateProperty(Property property) {
        List<Property> properties = retrieveProperties(property.getOfficeId(), property.getCategory(), property.getName());
        if (properties.isEmpty()) {
            throw new NotFoundException("Could not find property to update.");
        }
        connection(dsl, conn -> {
            setOffice(conn, property.getOfficeId());
            CWMS_PROPERTIES_PACKAGE.call_SET_PROPERTY(DSL.using(conn).configuration(), property.getCategory(),
                    property.getName(), property.getValue(), property.getComment(), property.getOfficeId());
        });
    }

    public void storeProperty(Property property) {
        connection(dsl, conn -> {
            setOffice(conn, property.getOfficeId());
            CWMS_PROPERTIES_PACKAGE.call_SET_PROPERTY(DSL.using(conn).configuration(), property.getCategory(),
                    property.getName(), property.getValue(), property.getComment(), property.getOfficeId());
        });
    }

    public void deleteProperty(String office, String category, String name) {
        List<Property> properties = retrieveProperties(office, category, name);
        if (properties.isEmpty()) {
            throw new NotFoundException("Could not find property to delete.");
        }
        connection(dsl, conn -> {
            setOffice(conn, office);
            CWMS_PROPERTIES_PACKAGE.call_DELETE_PROPERTY(DSL.using(conn).configuration(), category, name, office);
        });
    }
}
