package cwms.radar.data.dao;


import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.radar.data.dto.rating.RatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadataList;
import fixtures.RadarApiSetupCallback;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
class RatingMetadataDaoTestIT {

// This is how it can be run from an integration test using docker etc.
// It takes 8 minutes or so to run it this way.
    @Test
    void testRetrieveMetadata() throws SQLException, JsonProcessingException {

        CwmsDatabaseContainer databaseLink = RadarApiSetupCallback.getDatabaseLink();
        databaseLink.connection((Consumer<Connection>) c -> {//
            testRetrieveMetadata(c, databaseLink.getOfficeId());
        });
    }


//    // This is how the test can be run from a unit test.
//    // Must have existing database and specify the -D connection args
//
//    @Test
//    void testRetrieveMetadata() throws SQLException, JsonProcessingException
//    {
//        Connection c = getConnection();
//        testRetrieveMetadata(c, "SWT");
//    }


    void testRetrieveMetadata(Connection c, String connectionOfficeId) {
        try (DSLContext context = getDslContext(c, connectionOfficeId)) {

            String[] files = new String[]{
                    "ALBT.Stage_Stage-Corrected.Linear.USGS-NWIS.xml.gz",
                    "ARBU.Elev_Stor.Linear.Production.xml.gz",
                    "BEAV.Stage_Flow.BASE.PRODUCTION.xml",
                    "CE73D618.Stage_Flow.EXSA.PRODUCTION.gz",
                    "CLAK.Stage_Flow.EXSA.PRODUCTION.gz",
                    "CYCK1.Stage_Flow.EXSA.PRODUCTION.gz",
                    "DE36968A.Stage_Flow.EXSA.PRODUCTION.gz",
                    "EKLY.Stage_Stage-Corrected.Linear.USGS-NWIS.xml.gz",
                    "ELME.Stage_Stage-Corrected.Linear.USGS-NWIS.xml.gz",
                    "LENA.Stage_Flow.BASE.PRODUCTION.xml.gz",
            };

            long start = System.currentTimeMillis();

            storeRatings(c, files);

            long end = System.currentTimeMillis();
            System.out.println("Store Ratings took " + (end - start) + "ms");

            String officeId = "SWT";
            RatingMetadataDao dao = new RatingMetadataDao(context);

            // This page is good for how to build regex like masks
            // https://docs.oracle.com/database/121/SQLRF/ap_posix001.htm#SQLRF55540
            RatingMetadataList firstPage = dao.retrieve(null, 50, officeId, "ALBT[.]Stage.*");
            assertNotNull(firstPage);

            assertTrue(firstPage.getSize() == 47);

            String nextPage = firstPage.getNextPage();
            assertTrue(nextPage == null || nextPage.isEmpty());

            firstPage = dao.retrieve(null, 25, officeId, "ALBT[.]Stage.*");
            assertNotNull(firstPage);
            nextPage = firstPage.getNextPage();
            RatingMetadataList secondPage = dao.retrieve(nextPage, 25, officeId, "ALBT[.]Stage.*");
            assertNotNull(secondPage);
            nextPage = secondPage.getNextPage();
            assertTrue(nextPage == null || nextPage.isEmpty());

            String mask = "*";
            firstPage = dao.retrieve(null, 5, officeId, mask);
            assertNotNull(firstPage);

            assertEquals(5, firstPage.getSize());

            List<RatingMetadata> metadata = firstPage.getRatingMetadata();
            assertNotNull(metadata);
            assertFalse(metadata.isEmpty());

            nextPage = firstPage.getNextPage();
            assertNotNull(nextPage);
            assertFalse(nextPage.isEmpty());

            secondPage = dao.retrieve(nextPage, 5, officeId, mask);
            assertNotNull(secondPage);
            assertFalse(secondPage.getSize() == 0);

            List<RatingMetadata> secondMetadata = secondPage.getRatingMetadata();
            assertNotNull(secondMetadata);

        } catch (RatingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void storeRatings(Connection c, String[] files) throws IOException, RatingException {

        for(String filename : files){
            storeRatingSet(c, "cwms/radar/data/dao/" + filename);
        }
    }

    private static void storeRatingSet(Connection c, String resource) throws IOException,
            RatingException {
        String xmlRating = JsonRatingUtilsTest.loadResourceAsString(resource);
        // make sure we got something.
        assertNotNull(xmlRating);

        // make sure we can parse it.
        RatingSet ratingSet = RatingSet.fromXml(xmlRating);
        assertNotNull(ratingSet);

        ratingSet.storeToDatabase(c, true) ;
    }


}