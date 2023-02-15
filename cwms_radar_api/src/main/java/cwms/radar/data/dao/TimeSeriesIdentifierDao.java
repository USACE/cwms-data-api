package cwms.radar.data.dao;

import cwms.radar.data.dto.TimeSeriesIdentifier;
import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

public class TimeSeriesIdentifierDao extends JooqDao<TimeSeriesIdentifier> {


    public TimeSeriesIdentifierDao(DSLContext dsl) {
        super(dsl);
    }


    public List<TimeSeriesIdentifier> getTimeSeriesIdentifiers(String office) {
        return null;
    }

    public Optional<TimeSeriesIdentifier> getTimeSeriesIdentifier(String office, String timeseriesId) {
        return null;
    }

    public void rename (String officeId, String origId, String newId){
        CWMS_TS_PACKAGE.call_RENAME_TS(dsl.configuration(), officeId, origId, newId);
    }

    public void deleteAll(String officeId, String tsId){
        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteAll(connection, officeId, tsId);
        });
    }
}
