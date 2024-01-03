package cwms.cda.data.dao.texttimeseries;


import static cwms.cda.data.dao.DaoTest.getDslContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.timeseriestext.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.StandardTextId;
import cwms.cda.data.dto.timeseriestext.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
import cwms.cda.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class StandardTimeSeriesTextDaoTestIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/store_std_text_timeseries.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_std_text_timeseries.sql");
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

        StandardTextId standardTextId = null;

        // The delete script deletes from 2005-02-01 13:30:00 - 2005-02-02 17:00:00'

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-02-01T15:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-01T23:00:00Z");

        //  make sure it doesn't exist

        Date startDate = Date.from(startZDT.toInstant());
        Date endDate = Date.from(endZDT.toInstant());
        Date versionDate = null;
        boolean maxVersion = false;
        boolean retText = true;
        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId, standardTextId,
                startDate, endDate, versionDate,
                maxVersion, retText, minAttr, maxAttr);

        Assertions.assertNotNull(tts);
        Assertions.assertEquals(0, tts.getStandardTextValues().size());

        // create/store
        StandardTextTimeSeriesRow row = new StandardTextTimeSeriesRow.Builder()
                .withDateTime(startDate)
                .withOfficeId("CWMS")
                .withStandardTextId("A")
                .build();
        dao.store(officeId, tsId,  row, true, true);

        // retrieve and verify
        tts = dao.retrieveTextTimeSeries(officeId, tsId, null,
                startDate, endDate, versionDate,
                maxVersion, retText, minAttr, maxAttr);

        Assertions.assertNotNull(tts);
        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        Assertions.assertFalse(stdRows.isEmpty());
        Assertions.assertEquals(1, stdRows.size());
//        logger.atInfo().log("got %d rows", stdRows.size());
        StandardTextTimeSeriesRow first = stdRows.iterator().next();
        Assertions.assertEquals("A", first.getStandardTextId());

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

    private static void testRetrieve(StandardTimeSeriesTextDao dao) throws JsonProcessingException {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        StandardTextId standardTextId = null;
        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00-07:00[PST8PDT]");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-03T08:00:00-07:00[PST8PDT]");

        Date startDate = Date.from(startZDT.toInstant());
        Date endDate = Date.from(endZDT.toInstant());
        Date versionDate = null;
        boolean maxVersion = false;
        boolean retText = true;
        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTextTimeSeries(officeId, tsId, standardTextId,
                startDate, endDate, versionDate,
                maxVersion, retText, minAttr, maxAttr);

        Assertions.assertNotNull(tts);

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        Assertions.assertNotNull(stdRows);
        Assertions.assertFalse(stdRows.isEmpty());

        StandardTextTimeSeriesRow first = stdRows.iterator().next();
        Assertions.assertNotNull(first);
        Assertions.assertEquals("E", first.getStandardTextId());

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNull(regRows);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(tts);
        Assertions.assertNotNull(json);


    }

}