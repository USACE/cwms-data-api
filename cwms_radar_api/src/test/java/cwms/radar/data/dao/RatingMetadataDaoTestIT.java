package cwms.radar.data.dao;

import static cwms.radar.data.dao.DaoTest.getDslContext;
import static cwms.radar.data.dao.JsonRatingUtilsTest.readFully;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codahale.metrics.MetricRegistry;

import cwms.radar.api.DataApiTestIT;
import cwms.radar.data.dto.rating.AbstractRatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadata;
import cwms.radar.data.dto.rating.RatingMetadataList;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import hec.data.RatingException;
import hec.data.cwmsRating.AbstractRating;
import hec.data.cwmsRating.RatingSet;
import hec.data.cwmsRating.RatingSpec;
import hec.data.rating.IRatingSpecification;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

//@Disabled("Needs larger rework for auth system changes.")
@Tag("integration")
class RatingMetadataDaoTestIT extends DataApiTestIT {

// This is how it can be run from an integration test using docker etc.
// It takes 8 minutes or so to run it this way.
    @BeforeAll
    public static void swt_permissions() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = RadarApiSetupCallback.getDatabaseLink();
        addUserToGroup(databaseLink.getUsername(), "CWMS Users", "SWT");
        addUserToGroup(databaseLink.getUsername(), "TS ID Creator", "SWT");
    }

    @AfterAll
    public static void remove_swt_permissiosn() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = RadarApiSetupCallback.getDatabaseLink();
        removeUserFromGroup(databaseLink.getUsername(), "CWMS Users", "SWT");
        removeUserFromGroup(databaseLink.getUsername(), "TS ID Creator", "SWT");
    }

    @Test
    void testRetrieveMetadata() throws SQLException {

        CwmsDatabaseContainer<?> databaseLink = RadarApiSetupCallback.getDatabaseLink();
        databaseLink.connection((Consumer<Connection>) c -> {//
            testRetrieveMetadata(c, "SWT");
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
            RatingMetadataDao dao = new RatingMetadataDao(context, new MetricRegistry());

            // This page is good for how to build regex like masks
            // https://docs.oracle.com/database/121/SQLRF/ap_posix001.htm#SQLRF55540

            RatingMetadataList firstPage = dao.retrieve(null, 50, officeId, "ALBT[.]Stage.*", null, null);
            assertNotNull(firstPage);

            System.out.println("First Page size: " + firstPage.getSize());

            assertTrue(firstPage.getSize() >= 5 && firstPage.getSize() <= 50);

            String nextPage = firstPage.getNextPage();
            assertNotSame(nextPage, firstPage.getPage());

            firstPage = dao.retrieve(null, 25, officeId, "ALBT[.]Stage.*", null, null);
            assertNotNull(firstPage);
            nextPage = firstPage.getNextPage();
            RatingMetadataList secondPage = dao.retrieve(nextPage, 25, officeId, "ALBT[.]Stage.*", null, null);
            assertNotNull(secondPage);
            nextPage = secondPage.getNextPage();
            assertTrue(nextPage == null || nextPage.isEmpty());

            String mask = "*";
            firstPage = dao.retrieve(null, 5, officeId, mask, null, null);
            assertNotNull(firstPage);

            assertTrue(firstPage.getSize() >=5);

            List<RatingMetadata> metadata = firstPage.getRatingMetadata();
            assertNotNull(metadata);
            assertFalse(metadata.isEmpty());

            nextPage = firstPage.getNextPage();
            assertNotNull(nextPage);
            assertFalse(nextPage.isEmpty());

            secondPage = dao.retrieve(nextPage, 5, officeId, mask, null, null);
            assertNotNull(secondPage);
            assertNotEquals(0, secondPage.getSize());

            List<RatingMetadata> secondMetadata = secondPage.getRatingMetadata();
            assertNotNull(secondMetadata);

        } catch (RatingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void storeRatings(Connection c, String[] files) throws IOException, RatingException {

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
        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlRating);

        assertNotNull(ratingSet);

        RatingJdbcFactory.store(ratingSet, c, true, true);

    }

    @Test
    @Disabled("data file not available")
    void testParse() throws IOException, RatingException {
        String resourcePath = "cwms/radar/data/dao/swt_ratings.xml";

        InputStream stream =
                RatingMetadataDaoTestIT.class.getClassLoader().getResourceAsStream(resourcePath);
        assertNotNull(stream);

        String xmlText = readFully(stream);
        assertNotNull(xmlText);

        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlText);
        assertNotNull(ratingSet);

        AbstractRating[] ratings = ratingSet.getRatings();
        assertNotNull(ratings);

        RatingSpec ratingSpec = ratingSet.getRatingSpec();


        IRatingSpecification ratingSpecification = ratingSet.getRatingSpecification();

        System.out.println("Got " + ratings.length + " ratings");
    }


    @Test
    void testRetrieveRatings() throws SQLException  {
        String swt = "SWT";
        CwmsDatabaseContainer<?> databaseLink = RadarApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
            try (DSLContext lrl = getDslContext(c, swt)) {
                long start = System.nanoTime();
                RatingMetadataDao dao = new RatingMetadataDao(lrl, new MetricRegistry());

                String mask = "*";

                String office = "SWT";
                Set<String> ratingIds = dao.getRatingIds(office, mask, 0, 100);

                Map<cwms.radar.data.dto.rating.RatingSpec, Set<AbstractRatingMetadata>> got
                        = dao.getRatingsForIds(office, ratingIds, null, null);

                assertNotNull(got);

                // count how many ratings we got
                int count = got.values().stream().mapToInt(Set::size).sum();

                long end = System.nanoTime();
                long ms = (end - start) / 1000000;
                System.out.println("Got:" + got.size() + " count:" + count + " ratings in " + ms + "ms");
            }
        });
    }


}