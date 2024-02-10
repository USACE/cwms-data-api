package cwms.cda.data.dao.binarytimeseries;


import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.dao.CwmsDbLocJooq;


public class TimeSeriesBinaryDaoTest  {

    private static final String officeId = "SPK";
    private static final String locationId = "TsTextTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, officeId);

        createTimeseries(officeId, tsId, 0);  // offset needs to be valid for 1Hour
    }

    private static void createTimeseries(String office, String timeseries, int offset) throws SQLException {
        DSLContext dsl = getDslContext(getConnection(), officeId);

        dsl.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement("begin\n"
                    + "  cwms_ts.create_ts(?,?,?);\n"
                    + "end;")) {
                stmt.setString(1, office);
                stmt.setString(2, timeseries);
                stmt.setInt(3, offset);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's find for these tests.
                }
                throw new RuntimeException("Unable to create timeseries",ex);
            }
        });
    }

    private static void createLocation(String locationId, boolean b, String officeId) throws SQLException {

        DSLContext dsl = getDslContext(getConnection(), officeId);
        dsl.connection(connection -> {
            CwmsDbLocJooq locJooq = new CwmsDbLocJooq();
            locJooq.store(connection, officeId, locationId, null, null, "PST", null, null, null, null, null, null, null,
                    locationId, null, null, true, true);

        });
    }




    @Test
    void test_store_retrieve() throws SQLException {
        DSLContext dsl = getDslContext(getConnection(), "SPK");

        TimeSeriesBinaryDao dao = new TimeSeriesBinaryDao(dsl);

        String mask = "*";

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T14:00:00Z");
        Instant startInstant = startZDT.toInstant();
        Instant endInstant = endZDT.toInstant();
        Instant versionInstant = null;


        boolean retrieveBinary = true;
        Long minAttribute = null;
        Long maxAttribute = null;


        byte[] data = "digital data".getBytes();
        String binaryType = "application/octet-stream";


        // default is T, T, F, F
        boolean maxVersion = true;
        boolean storeExisting = true;
        boolean storeNonExisting = true;
        boolean replaceAll = true;

        Long attr = null;

        dao.store(officeId, tsId, data, binaryType, startInstant, endInstant, versionInstant,
                maxVersion, storeExisting, storeNonExisting, replaceAll, attr);


        List<TimeSeriesBinaryDao.BinaryRecord> records = dao.retrieve(officeId, tsId, mask,
                startInstant, endInstant, versionInstant, maxVersion, retrieveBinary,
                minAttribute, maxAttribute);
        assertNotNull(records);
        assertFalse(records.isEmpty());
        TimeSeriesBinaryDao.BinaryRecord firstRecord = records.get(0);
        assertNotNull(firstRecord);
    }


}