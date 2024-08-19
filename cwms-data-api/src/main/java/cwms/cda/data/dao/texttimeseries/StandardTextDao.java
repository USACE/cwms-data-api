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

package cwms.cda.data.dao.texttimeseries;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.StandardTextCatalog;
import cwms.cda.data.dto.texttimeseries.StandardTextId;
import cwms.cda.data.dto.texttimeseries.StandardTextValue;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_STD_TEXT;

import java.util.List;

import static java.util.stream.Collectors.toList;

public final class StandardTextDao extends JooqDao<StandardTextId> {
    public StandardTextDao(DSLContext dsl) {
        super(dsl);
    }

    public StandardTextCatalog retreiveStandardTextCatalog(String standardTextIdMask, String officeIdMask) {
        AV_STD_TEXT view = AV_STD_TEXT.AV_STD_TEXT;
        List<StandardTextValue> textValues = dsl.select(view.OFFICE_ID, view.STD_TEXT_ID, view.LONG_TEXT)
                .from(view)
                .where(JooqDao.caseInsensitiveLikeRegexNullTrue(view.OFFICE_ID, officeIdMask))
                .and(JooqDao.caseInsensitiveLikeRegexNullTrue(view.STD_TEXT_ID, standardTextIdMask))
                .stream()
                .map(r -> new StandardTextValue.Builder()
                        .withStandardText(r.get(view.LONG_TEXT))
                        .withId(new StandardTextId.Builder()
                                .withId(r.get(view.STD_TEXT_ID))
                                .withOfficeId(r.get(view.OFFICE_ID))
                                .build())
                        .build())
                .collect(toList());
        return new StandardTextCatalog.Builder()
                .withValues(textValues)
                .build();
    }

    public void deleteStandardText(String stdTextId, String officeId, String deleteAction) {
        connection(dsl, c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            CWMS_TEXT_PACKAGE.call_DELETE_STD_TEXT(configuration, stdTextId, deleteAction, officeId);
        });
    }

    public StandardTextValue retrieveStandardText(String stdTextId, String officeId) {
        String textValue = connectionResult(dsl, c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            return CWMS_TEXT_PACKAGE.call_RETRIEVE_STD_TEXT(configuration,
                    stdTextId, officeId);
        });
        StandardTextId id = new StandardTextId.Builder()
                .withId(stdTextId)
                .withOfficeId(officeId)
                .build();
        return new StandardTextValue.Builder()
                .withId(id)
                .withStandardText(textValue)
                .build();

    }

    public void storeStandardText(String stdTextId, String stdTextValue, String officeId, boolean failIfExists) {
        String failIfExistsString = formatBool(failIfExists);
        connection(dsl, c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            CWMS_TEXT_PACKAGE.call_STORE_STD_TEXT(configuration, stdTextId, stdTextValue, failIfExistsString, officeId);
        });
    }
}
