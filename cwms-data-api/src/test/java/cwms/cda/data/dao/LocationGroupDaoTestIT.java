package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.LocationGroup;
import fixtures.CwmsDataApiSetupCallback;
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
        loadSqlDataFromResource("cwms/cda/data/sql/store_loc_groups.sql");
    }

    @AfterAll
    public static void deload_data() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_loc_groups.sql");
    }

    @Test
    void testGetAllWithoutAssignedLocations() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c));
                List<LocationGroup> groups = dao.getLocationGroups("SPK");
                Optional<LocationGroup> group = groups.stream()
                    .filter(g -> "Test Group2".equals(g.getId()))
                    .findFirst();
                assertTrue(group.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testGetAllWithCategoryRegex() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c));
                List<LocationGroup> groups = dao.getLocationGroups("SPK", "Test Category3");
                Optional<LocationGroup> group3 = groups.stream()
                        .filter(g -> "Test Group3".equals(g.getId()))
                        .findFirst();
                Optional<LocationGroup> group2 = groups.stream()
                        .filter(g -> "Test Group2".equals(g.getId()))
                        .findFirst();
                assertTrue(group3.isPresent());
                assertFalse(group2.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testGetAllWithCategoryRegexStar() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c));
                List<LocationGroup> groups = dao.getLocationGroups("SPK", ".*Category3");
                Optional<LocationGroup> group3 = groups.stream()
                        .filter(g -> "Test Group3".equals(g.getId()))
                        .findFirst();
                Optional<LocationGroup> group2 = groups.stream()
                        .filter(g -> "Test Group2".equals(g.getId()))
                        .findFirst();
                assertTrue(group3.isPresent());
                assertFalse(group2.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testGetAllWithCategoryRegexStarFindsBoth() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c));
                List<LocationGroup> groups = dao.getLocationGroups("SPK", "Test Category.*");
                Optional<LocationGroup> group3 = groups.stream()
                        .filter(g -> "Test Group3".equals(g.getId()))
                        .findFirst();
                Optional<LocationGroup> group2 = groups.stream()
                        .filter(g -> "Test Group2".equals(g.getId()))
                        .findFirst();
                assertTrue(group3.isPresent());
                assertTrue(group2.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }



    @Test
    void testGetOneWithoutAssignedLocations() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                LocationGroupDao dao = new LocationGroupDao(dslContext(c));
                Optional<LocationGroup> group = dao.getLocationGroup("SPK", "Test Category2", "Test Group2");
                assertTrue(group.isPresent());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
