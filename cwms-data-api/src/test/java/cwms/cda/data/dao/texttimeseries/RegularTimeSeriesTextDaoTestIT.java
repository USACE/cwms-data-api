package cwms.cda.data.dao.texttimeseries;


import static cwms.cda.data.dao.DaoTest.getDslContext;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
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
class RegularTimeSeriesTextDaoTestIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/store_reg_text_timeseries.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_reg_text_timeseries.sql");
    }

    @Test
    void testCreate() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testCreate(dao);
                }
        );
    }

    private void testCreate(RegularTimeSeriesTextDao dao) {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        // store script creates from 02:30:00 - 07:00:00'
        // The delete script deletes from 0:30 t- 23:00
        // So if we look for rows outside the create range we should get nothing
        // we can then create those rows
        //  verify they are there and then
        // they will get cleaned up.


        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T14:00:00Z");

        //  make sure it doesn't exist

        Date startDate = Date.from(startZDT.toInstant());
        Date endDate = Date.from(endZDT.toInstant());
        Date versionDate = null;
        boolean maxVersion = false;

        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startDate, endDate, versionDate,
                maxVersion, minAttr, maxAttr);

        Assertions.assertNotNull(tts);
        Assertions.assertNull(tts.getRegularTextValues());  // finds 10,11,12,13,14 PST

        // create/store
        String testValue = "my awesome text ts";
        RegularTextTimeSeriesRow row = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(startDate)
        .withVersionDate(versionDate)
        .withAttribute(420L)
                .withTextValue(testValue)
                .build()
        ;

        dao.storeRow(officeId, tsId, row, true, true);


        // retrieve and verify
        tts = tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startDate, endDate, versionDate,
                maxVersion, minAttr, maxAttr);

        Assertions.assertNotNull(tts);
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertFalse(regRows.isEmpty());
        Assertions.assertEquals(1, regRows.size());
//        logger.atInfo().log("got %d rows", regRows.size());
        RegularTextTimeSeriesRow first = regRows.iterator().next();
        Assertions.assertEquals(testValue, first.getTextValue());

    }

    @Test
    void testRetrieve() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    RegularTimeSeriesTextDao dao = new RegularTimeSeriesTextDao(dsl);

                    testRetrieve(dao);
                }
        );
    }

    private static void testRetrieve(RegularTimeSeriesTextDao dao) {
        String officeId = "SPK";
        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";
        String testValue = "my awesome text ts";

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T03:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T07:00:00Z");

        Date startDate = Date.from(startZDT.toInstant());
        Date endDate = Date.from(endZDT.toInstant());
        Date versionDate = null;
        boolean maxVersion = false;

        Long minAttr = null;
        Long maxAttr = null;

        TextTimeSeries  tts = dao.retrieveTimeSeriesText(officeId, tsId, "*",
                startDate, endDate, versionDate,
                maxVersion, minAttr, maxAttr);

        Assertions.assertNotNull(tts);

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        Assertions.assertNotNull(regRows);
        Assertions.assertFalse(regRows.isEmpty());

        RegularTextTimeSeriesRow first = regRows.iterator().next();
        Assertions.assertNotNull(first);
        Assertions.assertEquals(testValue, first.getTextValue());

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        Assertions.assertNull(stdRows);

    }

}