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
import cwms.cda.data.dto.LookupType;

import fixtures.CwmsDataApiSetupCallback;
import helpers.DTOMatch;
import java.util.Optional;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
final class LookupTypeDaoTestIT extends DataApiTestIT {

    @Test
    void testLookupTypeRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        LookupType lookupType = new LookupType.Builder()
                .withOfficeId(databaseLink.getOfficeId())
                .withDisplayValue("Test Lookup Type")
                .withTooltip("Tooltip for test lookup type")
                .withActive(true)
                .build();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            LookupTypeDao lookupTypeDao = new LookupTypeDao(context);
            String category = "AT_EMBANK_STRUCTURE_TYPE";
            String prefix = "STRUCTURE_TYPE";

            // Store lookup type
            lookupTypeDao.storeLookupType(category, prefix, lookupType);
            List<LookupType> fromDbList = lookupTypeDao.retrieveLookupTypes(category, prefix, databaseLink.getOfficeId());
            LookupType fromDb = fromDbList.stream()
                    .filter(lt -> lt.getOfficeId().equals(lookupType.getOfficeId()) && lt.getDisplayValue().equals(lookupType.getDisplayValue()))
                    .findFirst()
                    .orElse(null);
            assertNotNull(fromDb, "LookupType retrieved from database does not match original lookupType");
            DTOMatch.assertMatch(lookupType, fromDb);

            // Delete lookup type
            lookupTypeDao.deleteLookupType(category, prefix, lookupType.getOfficeId(), lookupType.getDisplayValue());

            //Retrieve Deleted lookup type to confirm it no longer exists
            Optional<LookupType> empty = lookupTypeDao.retrieveLookupTypes(category, prefix, databaseLink.getOfficeId())
                    .stream()
                    .filter(lt -> lt.equals(lookupType))
                    .findAny();
            assertFalse(empty.isPresent(), "LookupType has been deleted, it should not show up in retrieve query");
        });
    }


}