package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dto.timeseriestext.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class TimeSeriesTextDaoTest extends DaoTest {

    @Test
    void retrieveFromView() throws SQLException {
        String OFFICE = "SPK";
        DSLContext dsl = getDslContext(OFFICE);

        assertNotNull(dsl);

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00-07:00[PST8PDT]");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-02-03T08:00:00-07:00[PST8PDT]");


        String tsId = "First519402.Flow.Inst.1Hour.0.1688755420497";

        Date startD = Date.from(startZDT.toInstant());
        Date endD = Date.from(endZDT.toInstant());
        Date verD = null;

        TimeSeriesTextDao dao = new TimeSeriesTextDao(dsl);
        TextTimeSeries textTimeSeries = dao.retrieveFromView("SPK", tsId,
                startD, endD, verD, null, null
        );

        assertNotNull(textTimeSeries);

        Collection<RegularTextTimeSeriesRow> regRows = textTimeSeries.getRegRows();
        assertFalse(regRows.isEmpty());

        Collection<StandardTextTimeSeriesRow> stdRows = textTimeSeries.getStdRows();
        assertFalse(stdRows.isEmpty());


    }
}