package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;

import cwms.radar.data.dto.Location;

public class LocationsDao extends Dao<Location> {

    public LocationsDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public List<Location> getAll(Optional<String> limitToOffice) {
        return null;
    }

    @Override
    public Optional<Location> getByUniqueName(String uniqueName, Optional<String> limitToOffice) {
        return null;
    }

}
