package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.formatters.json.JsonV1;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.geojson.FeatureCollection;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class LocationGroupDaoTest {

    @Test
    public void testGetOne() throws SQLException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");

        LocationGroupDao dao = new LocationGroupDao(lrl);
        List<LocationGroup> groups = dao.getLocationGroups();
        assertNotNull(groups);
        assertFalse(groups.isEmpty());


    }

    @Test
    void getLocationGroupsAtOffice() throws SQLException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");
        LocationGroupDao dao = new LocationGroupDao(lrl);
        List<LocationGroup> groups = dao.getLocationGroups("LRL");

        assertNotNull(groups);
        assertFalse(groups.isEmpty());


    }

    @Test
    void getLocationGroup() throws SQLException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");
        LocationGroupDao dao = new LocationGroupDao(lrl);
        LocationGroup group = dao.getLocationGroup("LRL", "Basin", "Green River Basin").get();

        assertNotNull(group);
        List<AssignedLocation> locs = group.getAssignedLocations();
        assertFalse(locs.isEmpty());

        List<Number> attr =
                locs.stream().map(AssignedLocation::getAttribute).collect(Collectors.toList());
        assertTrue(isSorted(attr));

        JsonV1 jsonV1 = new JsonV1();
        String json = jsonV1.format(group);

        assertNotNull(json);


    }

    public static boolean isSorted(List<Number> attributes) {
        if (attributes.isEmpty() || attributes.size() == 1) {
            return true;
        }

        Iterator<Number> iter = attributes.iterator();
        Number current, previous = iter.next();
        while (iter.hasNext()) {
            current = iter.next();

            if (current != null || previous != null) {
                if (current == null && previous != null) {
                    return false;
                } else if (current != null && previous == null) {
                    // fine assuming nulls come first
                } else {
                    if (previous.doubleValue() > current.doubleValue()) {
                        return false;
                    }
                }
            }
            previous = current;
        }
        return true;
    }


    @Test
    public void getLocationGroupAsGeojson() throws SQLException, JsonProcessingException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");
        LocationGroupDao dao = new LocationGroupDao(lrl);
        FeatureCollection fc = dao.buildFeatureCollectionForLocationGroup("LRL", "Basin",
                "Green River Basin", "EN");
        assertNotNull(fc);

        ObjectWriter ow = new ObjectMapper().writerWithDefaultPrettyPrinter();
        String json = ow.writeValueAsString(fc);

        assertNotNull(json);
    }


}