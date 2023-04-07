package cwms.radar.data.dao;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.radar.api.DataApiTestIT;
import cwms.radar.data.dto.LocationGroup;
import fixtures.RadarApiSetupCallback;
import java.util.List;
import java.util.Optional;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class LocationGroupDaoTestIT extends DataApiTestIT {

    @BeforeAll
    public static void load_data() throws Exception {
        loadSqlDataFromResource("cwms/radar/data/sql/store_loc_groups.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/radar/data/sql/store_loc_groups.sql");
    }

    @Test
    void testGetAllWithoutAssignedLocations() throws Exception {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c, "SPK"));
                List<LocationGroup> groups = dao.getLocationGroups("SPK");
                Optional<LocationGroup> group = groups.stream()
                    .filter(g -> "Test Group1".equals(g.getId()))
                    .findFirst();
                assertTrue(group.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testGetOneWithoutAssignedLocations() throws Exception {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c, "SPK"));
                Optional<LocationGroup> group = dao.getLocationGroup("SPK", "Test Category2", "Test Group2");
                assertTrue(group.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
