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

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.Property;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
final class PropertyDaoTestIT  extends DataApiTestIT {

    @Test
    void testPropertyRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        Property property = new Property.Builder()
                .withCategory("PropertyDaoTestIT")
                .withOfficeId(databaseLink.getOfficeId())
                .withName("testPropertyRoundTrip")
                .withValue("value")
                .withComment("CDA integration test property")
                .build();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            PropertyDao propertyDao = new PropertyDao(context);
            propertyDao.storeProperty(property);
            Property fromDb = propertyDao.retrieveProperty(property.getOfficeId(),
                    property.getCategory(), property.getName(), null);
            assertEquals(property, fromDb, "Property retrieved from database does not match original property");
            Property fromDbMulti = propertyDao.retrieveProperties(databaseLink.getOfficeId(), property.getCategory(), property.getName())
                    .get(0);
            assertEquals(property, fromDbMulti, "Property multi retrieved from database does not match original property");
            propertyDao.deleteProperty(databaseLink.getOfficeId(), property.getCategory(), property.getName());
            Property deleted = propertyDao.retrieveProperty(property.getOfficeId(),
                    property.getCategory(), property.getName(), null);
            assertNull(deleted.getValue(), "Property has been deleted, value should be null");
            List<Property> empty = propertyDao.retrieveProperties(databaseLink.getOfficeId(), property.getCategory(), property.getName());
            assertTrue(empty.isEmpty(), "Property has been deleted, it should not show up in multi retrieve query");
        });
    }
}
