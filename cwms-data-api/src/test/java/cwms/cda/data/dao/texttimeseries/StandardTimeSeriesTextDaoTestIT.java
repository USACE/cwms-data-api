package cwms.cda.data.dao.texttimeseries;


import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextId;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class StandardTimeSeriesTextDaoTestIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String LOAD_RESOURCE = "cwms/cda/data/sql/store_std_text_timeseries.sql";
    public static final String DELETE_RESOURCE = "cwms/cda/data/sql/delete_std_text_timeseries.sql";

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
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    StandardTimeSeriesTextDao dao = new StandardTimeSeriesTextDao(dsl);

                    testCreate(dao);
                }
        );
    }

    private void testCreate(StandardTimeSeriesTextDao dao) {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        // The basic structure of the test is to:
        // 1.  make sure the row doesn't exist
        // 2.  create/store the row
        // 3.  retrieve the row and verify it is there
        // The after-all will clean-up the data as long as we create it in the test time range.
        // The delete script deletes from 2005-02-01 13:30:00 - 2005-02-02 17:00:00'

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-02-01T15:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-01T23:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;

        // Step 1: make sure it doesn't exist
        Date startDate = Date.from(startZDT.toInstant());

        boolean maxVersion = false;
        boolean retText = true;
        Long minAttr = null;
        Long maxAttr = null;

        String idMask = "*";

        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId,  idMask,
                startInstant, endInstant, versionInstant,
                maxVersion, retText, minAttr, maxAttr);

        assertNotNull(tts);
        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        assertTrue(stdRows == null || stdRows.isEmpty());


        // Step 2: create/store
        StandardTextTimeSeriesRow row = new StandardTextTimeSeriesRow.Builder()
                .withDateTime(startDate)
                .withOfficeId("CWMS")
                .withStandardTextId("A")
                .build();
        dao.store(officeId, tsId,  row, true, true);

        // Step 3: retrieve and verify
        tts = dao.retrieveTextTimeSeries(officeId, tsId, idMask,
                startInstant, endInstant, versionInstant,
                maxVersion, retText, minAttr, maxAttr);

        assertNotNull(tts);
        stdRows = tts.getStandardTextValues();
        Assertions.assertFalse(stdRows.isEmpty());
        assertEquals(1, stdRows.size());
//        logger.atInfo().log("got %d rows", stdRows.size());
        StandardTextTimeSeriesRow first = stdRows.iterator().next();
        assertEquals("A", first.getStandardTextId());

    }

    @Test
    void testRetrieve() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    StandardTimeSeriesTextDao dao = new StandardTimeSeriesTextDao(dsl);

                    try {
                        testRetrieve(dao);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private  void testRetrieve(StandardTimeSeriesTextDao dao) throws JsonProcessingException {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        StandardTextId standardTextId = null;
        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00-07:00[PST8PDT]");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-03T08:00:00-07:00[PST8PDT]");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;

        boolean maxVersion = false;
        boolean retText = true;
        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId, standardTextId,
                startInstant, endInstant, versionInstant,
                maxVersion, retText, minAttr, maxAttr);

        assertNotNull(tts);

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        assertNotNull(stdRows);
        Assertions.assertFalse(stdRows.isEmpty());

        StandardTextTimeSeriesRow first = stdRows.iterator().next();
        assertNotNull(first);
        assertEquals("E", first.getStandardTextId());

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNull(regRows);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(tts);
        assertNotNull(json);


    }

    @Test
    void testDelete() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    StandardTimeSeriesTextDao dao = new StandardTimeSeriesTextDao(dsl);
                    testDelete(dao);
                }
        );
    }

    private void testDelete(StandardTimeSeriesTextDao dao) {
        // Structure of the test is:
        // 1) retrieve some data and verify its there
        // 2) delete it
        // 3) retrieve it again and verify its gone

        // Step 1: retrieve some data
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        // The load_data script is supposed to be storing between:
        // ('2005-02-01 13:30:00',
        // ('2005-02-02 17:00:00',
        // but we're only finding  Mon Jan 31 16:00:00 PST 2005  and Mon Jan 31 17:00:00 PST 2005
        // Pretty sure "Mon Jan 31 16:00:00 PST 2005" is the same a "Tue Feb 1 00:00:00 GMT 2005"

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-02-01T00:00:00Z");
        ZonedDateTime endZDT   = ZonedDateTime.parse("2005-02-01T01:30:00Z"); // expand a little so we get the second point

        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();

        boolean retText = true;

        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId, "*",
                startInstant, endInstant, null,
                false, retText, null, null);
        assertNotNull(tts);
        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        assertNotNull(stdRows, "Not finding the std rows that should be inserted by " + LOAD_RESOURCE);
        assertEquals(2, stdRows.size());
        StandardTextTimeSeriesRow[] stdArray = stdRows.toArray(new StandardTextTimeSeriesRow[0]);
        StandardTextTimeSeriesRow first = stdArray[0];
        assertNotNull(first);
        assertEquals(Date.from(startZDT.toInstant()), first.getDateTime());
        StandardTextTimeSeriesRow second = stdArray[1];
        assertNotNull(second);
        assertEquals(Date.from(ZonedDateTime.parse("2005-02-01T01:00:00Z").toInstant()), second.getDateTime());

        // Step 2: delete it
        dao.delete(officeId, tsId, "*", startInstant, endInstant, null, false, null, null);

        // Step 3: retrieve it again and verify its gone
        tts = dao.retrieveTextTimeSeries(officeId, tsId, "*",
                startInstant, endInstant, null,
                false, retText, null, null);
        assertNotNull(tts);
        stdRows = tts.getStandardTextValues();
        Assertions.assertNull(stdRows);


    }

    @Test
    void testStore() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    StandardTimeSeriesTextDao dao = new StandardTimeSeriesTextDao(dsl);
                    testStore(dao);
                }
        );
    }

    private void testStore(StandardTimeSeriesTextDao dao) {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        // Structure of the test is:
        // 1) retrieve some data and verify its there
        // 2) update it
        // 3) retrieve it again and verify its updated

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00-07:00[PST8PDT]");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-03T08:00:00-07:00[PST8PDT]");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;

        boolean maxVersion = false;
        boolean retText = true;
        Long minAttr = null;
        Long maxAttr = null;

        StandardTextId standardTextId = null;
        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId, standardTextId,
                startInstant, endInstant, versionInstant,
                maxVersion, retText, minAttr, maxAttr);
        assertNotNull(tts);

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        assertNotNull(stdRows);
        Assertions.assertFalse(stdRows.isEmpty());

        StandardTextTimeSeriesRow first = stdRows.iterator().next();
        assertNotNull(first);
        assertEquals("E", first.getStandardTextId());

        // Step 2: update it
        String updatedId = "A";
        StandardTextTimeSeriesRow row = new StandardTextTimeSeriesRow.Builder()
                .from(first)
                .withStandardTextId(updatedId)
                .build();
        dao.store(officeId, tsId,  row, true, true);

        // Step 3: retrieve it again and verify its updated
        Assertions.assertNotNull(tts);
        stdRows = tts.getStandardTextValues();
        Assertions.assertNotNull(stdRows);
        Assertions.assertFalse(stdRows.isEmpty());
        first = stdRows.iterator().next();
        Assertions.assertNotNull(first);

        Assertions.assertEquals(updatedId, first.getTextValue());
    }


}