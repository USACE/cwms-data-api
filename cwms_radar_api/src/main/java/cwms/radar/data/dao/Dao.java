package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;

public abstract class Dao<T> {
    @SuppressWarnings("unused")
    protected DSLContext dsl = null;

    public Dao(DSLContext dsl) {
        this.dsl = dsl;
    }

    public abstract List<T> getAll(Optional<String> limitToOffice);

    public abstract Optional<T> getByUniqueName( String uniqueName, Optional<String> limitToOffice);

}
