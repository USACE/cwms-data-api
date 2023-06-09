/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import cwms.cda.data.dto.rating.RatingTemplate;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;


@Disabled
class RatingTemplateDaoTest
{
	public static final String OFFICE_ID = "SWT";


    // This is how the test can be run from a unit test.
    // Must have existing database and specify the -D connection args
    // It takes 4.2 seconds to run it this way.
    @Test
    void testRetrieveRatingTemplates() throws SQLException
    {
        Connection c = getConnection();
        testRetrieveRatingTemplate(c, "SWT");
    }


    void testRetrieveRatingTemplate(Connection c, String connectionOfficeId) {
        try (DSLContext context = getDslContext(c, connectionOfficeId)) {

            String filename = "ARBU.Elev_Stor.Linear.Production.xml.gz";
            String resource = "cwms/cda/data/dao/" + filename;
            storeRatingSet(c, resource);

            String officeId = "SWT";
            RatingTemplateDao dao = new RatingTemplateDao(context);
            Set<RatingTemplate> ratingTemplates = dao.retrieveRatingTemplates(officeId,
                    "Elev;Stor.Linear");
            assertNotNull(ratingTemplates);
            assertFalse(ratingTemplates.isEmpty());

            // The ARBU rating template looks like:
            //  <rating-template office-id="SWT">
            //  <parameters-id>Elev;Stor</parameters-id>
            //  <version>Linear</version>
            //  <ind-parameter-specs>
            //   <ind-parameter-spec position="1">
            //    <parameter>Elev</parameter>
            //    <in-range-method>LINEAR</in-range-method>
            //    <out-range-low-method>NEAREST</out-range-low-method>
            //    <out-range-high-method>NEAREST</out-range-high-method>
            //   </ind-parameter-spec>
            //  </ind-parameter-specs>
            //  <dep-parameter>Stor</dep-parameter>
            //  <description></description>
            // </rating-template>

            boolean allSWT = ratingTemplates.stream().allMatch(rt ->
                    "SWT".equals(rt.getOfficeId())
                    && "Linear".equals(rt.getVersion() )
                    && "Stor".equals(rt.getDependentParameter())
                    && "Elev".equals(rt.getIndependentParameterSpecs().get(0).getParameter())
            );
            assertTrue(allSWT);

        } catch (RatingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void storeRatingSet(Connection c, String resource) throws IOException,
            RatingException {
        String xmlRating = JsonRatingUtilsTest.loadResourceAsString(resource);
        // make sure we got something.
        assertNotNull(xmlRating);

        // make sure we can parse it.
        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlRating);
        assertNotNull(ratingSet);

        RatingJdbcFactory.store(ratingSet,c, true, true);
    }


}