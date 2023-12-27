package cwms.cda.data.dao.texttimeseries;



import cwms.cda.api.DataApiTestIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class RegularTimeSeriesTextDaoTestIT extends DataApiTestIT {
    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/store_reg_txt_timeseries.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_reg_txt_timeseries.sql");
    }

    @Test
    void testRetrieve(){

    }
}