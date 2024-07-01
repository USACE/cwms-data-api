package cwms.cda.data.dao;

import cwms.cda.api.DataApiTestIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag("integration")
class TimeSeriesTextDaoTestIT extends DataApiTestIT {

    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/store_std_text_timeseries.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_std_text_timeseries.sql");
    }


}
