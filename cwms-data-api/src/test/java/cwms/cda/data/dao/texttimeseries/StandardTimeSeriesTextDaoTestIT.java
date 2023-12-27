package cwms.cda.data.dao.texttimeseries;



import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.Assert.assertNotNull;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.timeseriestext.StandardTextId;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Date;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class StandardTimeSeriesTextDaoTestIT extends DataApiTestIT {
    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/store_std_text_timeseries.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_std_text_timeseries.sql");
    }

    @Test
    void testRetrieve() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {//
                    DSLContext dsl = getDslContext(c, "SPK");
                    StandardTimeSeriesTextDao dao = new StandardTimeSeriesTextDao(dsl);

                    testRetrieve(dao);
                }
            );
        }

    private static void testRetrieve(StandardTimeSeriesTextDao dao) {
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

        assertNotNull(tts);
    }

}