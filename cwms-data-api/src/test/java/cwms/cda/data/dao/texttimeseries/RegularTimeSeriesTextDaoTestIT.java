package cwms.cda.data.dao.texttimeseries;


import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.helpers.ReplaceUtils;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.NoDataFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class RegularTimeSeriesTextDaoTestIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String LOAD_RESOURCE = "cwms/cda/data/sql/store_reg_text_timeseries.sql";
    public static final String DELETE_RESOURCE = "cwms/cda/data/sql/delete_reg_text_timeseries.sql";
    public static final String EXPECTED_TEXT_VALUE = "my awesome text ts";  // must match
    // store_reg_text_timeseries.sql

    private static final String officeId = "SPK";
    private static final String locationId = "TsTextTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, officeId);

        createTimeseries(officeId, tsId, 0);  // offset needs to be valid for 1Hour
    }

    @BeforeEach
    public void load_data() throws Exception {
        loadSqlDataFromResource(LOAD_RESOURCE);
    }

    @AfterEach
    public void deload_data() throws Exception {
        loadSqlDataFromResource(DELETE_RESOURCE);
    }

    @Test
    void testCreate() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                    DSLContext dsl = getDslContext(c, officeId);
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testCreate(dao);
                }
        );
    }


    private void testCreate(RegularTimeSeriesTextDao dao) {


        // store script creates from 02:30:00 - 07:00:00'
        // The delete script deletes from 0:30 t- 23:00
        // So if we look for rows outside the create range we should get nothing
        // we can then create those rows
        //  verify they are there and then
        // they will get cleaned up.


        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T14:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;

        //  make sure it doesn't exist
        boolean maxVersion = false;

        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, versionInstant, 64, new ReplaceUtils.OperatorBuilder());

        Assertions.assertNotNull(tts);
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        assertTrue(regRows.isEmpty());


        // create/store
        String testValue = EXPECTED_TEXT_VALUE;
        RegularTextTimeSeriesRow row = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(startInstant)
                .withTextValue(testValue)
                .build();

        dao.storeRows(officeId, tsId, true, Collections.singletonList(row), versionInstant);


        // retrieve and verify
        tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, versionInstant,
                64, new ReplaceUtils.OperatorBuilder());

        Assertions.assertNotNull(tts);
        regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertEquals(1, regRows.size());

        RegularTextTimeSeriesRow first = regRows.iterator().next();
        Assertions.assertEquals(testValue, first.getTextValue());

    }

    @Test
    void testRetrieve() throws SQLException {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, officeId);
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testRetrieve(dao);
                }
        );
    }

    private static void testRetrieve(RegularTimeSeriesTextDao dao) {

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T07:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();

        boolean maxVersion = false;

        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, null,
                64, new ReplaceUtils.OperatorBuilder());

        Assertions.assertNotNull(tts);

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertFalse(regRows.isEmpty(), "retrieve should find the rows inserted by "
                + "store_reg_text_timeseries.sql");

        RegularTextTimeSeriesRow first = regRows.iterator().next();
        Assertions.assertNotNull(first);
        Assertions.assertEquals(EXPECTED_TEXT_VALUE, first.getTextValue());


    }

    @Test
    void testDelete() throws SQLException {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, officeId);
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testDelete(dao);
                }
        );
    }

    private void testDelete(RegularTimeSeriesTextDao dao) {


        // Structure of the test is:
        // 1) retrieve some data and verify its there
        // 2) delete it
        // 3) retrieve it again and verify its gone

        // Step 1: retrieve some data
        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T04:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;
        boolean maxVersion = false;

        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, null,
                64, new ReplaceUtils.OperatorBuilder());

        Assertions.assertNotNull(tts);
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertFalse(regRows.isEmpty(), "testDelete must first find the rows inserted "
                + "by store_reg_text_timeseries.sql");

        // Step 2: delete it
        dao.delete(officeId, tsId, "*", startInstant, endInstant, null);

        // Step 3: retrieve it again and verify its gone
        tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, versionInstant,
                64, new ReplaceUtils.OperatorBuilder());
        Assertions.assertNotNull(tts);
        regRows = tts.getRegularTextValues();
        assertNotNull(regRows);
        assertTrue(regRows.isEmpty());

    }


    @Test
    void testStore() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, officeId);
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testStore(dao);
                }
        );
    }

    private void testStore(RegularTimeSeriesTextDao dao) {

        // Structure:
        // 1) retrieve some data and verify its there
        // 2) update it
        // 3) retrieve it again and verify its updated

        // Step 1: retrieve some data
        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T04:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;

        TextTimeSeries tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, versionInstant,
                64, new ReplaceUtils.OperatorBuilder());

        Assertions.assertNotNull(tts);
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertFalse(regRows.isEmpty(), "Before trying to store we should first be "
                + "finding the rows inserted by store_reg_text_timeseries.sql");


        RegularTextTimeSeriesRow first = regRows.iterator().next();
        Assertions.assertNotNull(first);
        Assertions.assertEquals(EXPECTED_TEXT_VALUE, first.getTextValue());
        //fyi - first.getVersionDate() // Sat Nov 11 00:00:00 PST 1111 - -WEIRD

        // Step 2: update it
        String updatedValue = "my new textValue";
        RegularTextTimeSeriesRow row = new RegularTextTimeSeriesRow.Builder()
                .from(first)
                .withTextValue(updatedValue)
                .build();

        // fyi the date.toSTring is: "Fri Dec 31 19:00:00 PST 2004" or: 2004-12-31T19:00:00
        // .000-0800  which matches up to 3am 1/1 2005 in UTC.  looks right.
        // pl/sql default for maxVersion is true
        // and for replaceAll its false;  But we do want it to replaceAll to update the value.
        dao.storeRows(officeId, tsId, true, Collections.singletonList(row), versionInstant);

        // Step 3: retrieve it again and verify its updated
        tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startInstant, endInstant, versionInstant,
                64, new ReplaceUtils.OperatorBuilder());
        Assertions.assertNotNull(tts);
        regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertFalse(regRows.isEmpty());
        first = regRows.iterator().next();
        Assertions.assertNotNull(first);
        Assertions.assertEquals(updatedValue, first.getTextValue());
    }


    @Test
    void testRetrieveLocDoesntExist() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, officeId);
            RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

            ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
            ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T07:00:00Z");
            Instant startInstant = startZDT.toInstant();
            Instant endInstant = endZDT.toInstant();

            NotFoundException thrown = assertThrows(NotFoundException.class, () -> {
                dao.retrieveTimeSeriesText(officeId, "ASDFASFDASDF.Flow"
                                + ".Inst.1Hour.0.raw", "*",
                        startInstant, endInstant, null,
                        64, new ReplaceUtils.OperatorBuilder());
            });

            Throwable cause = thrown.getCause();
            assertInstanceOf(SQLException.class, cause);
            String message = cause.getMessage();
            assertTrue(message.contains("ORA-20025"));
            assertTrue(message.contains("LOCATION_ID_NOT_FOUND"));
        });
    }

    @Test
    void testRetrieveBadF() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, officeId);
            RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

            ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
            ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T07:00:00Z");
            Instant startInstant = startZDT.toInstant();
            Instant endInstant = endZDT.toInstant();

            String badFId = tsId.replace(".raw", ".asdfasdf");

            NotFoundException thrown = assertThrows(NotFoundException.class, () -> {

                dao.retrieveTimeSeriesText(officeId, badFId, "*",
                        startInstant, endInstant, null,
                        64, new ReplaceUtils.OperatorBuilder());
            });

            Throwable cause = thrown.getCause();
            assertInstanceOf(SQLException.class, cause);
            String message = cause.getMessage();

            assertTrue(message.contains("TS_ID_NOT_FOUND"));
        });
    }


}