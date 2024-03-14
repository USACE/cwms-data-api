package cwms.cda.data.dao.binarytimeseries;


import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeriesRow;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class TimeSeriesBinaryDaoTestIT extends DataApiTestIT {

    private static final String officeId = "SPK";
    private static final String locationId = "TsTextTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, officeId);

        createTimeseries(officeId, tsId, 0);  // offset needs to be valid for 1Hour
    }


    @Test
    void test_store_retrieve() throws SQLException {
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            DSLContext dsl = getDslContext(c, "SPK");

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



            dao.store(officeId, tsId, data, binaryType, startInstant, endInstant, versionInstant,
                    maxVersion, storeExisting, storeNonExisting, replaceAll);

            List<BinaryTimeSeriesRow> records = dao.retrieveRows(officeId, tsId, mask,
                    startInstant, endInstant, versionInstant, maxVersion, retrieveBinary,
                    minAttribute, maxAttribute);
            assertNotNull(records);
            assertFalse(records.isEmpty());
            BinaryTimeSeriesRow firstRecord = records.get(0);
            assertNotNull(firstRecord);
        });
    }

    @Test
    void test_bts_store_retrieve() throws SQLException {

        ZonedDateTime startZDT = ZonedDateTime.parse("2005-01-01T08:00:00Z");
        ZonedDateTime endZDT = ZonedDateTime.parse("2005-01-01T14:00:00Z");

        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            DSLContext dsl = getDslContext(c, "SPK");
            TimeSeriesBinaryDao dao = new TimeSeriesBinaryDao(dsl);

            dao.delete(officeId, tsId, "*", startZDT, endZDT, null, true, null, null);

            BinaryTimeSeries got = dao.retrieve(officeId, tsId, "*", startZDT.toInstant(), endZDT.toInstant(), null, true, true, null, null);
            assertNotNull(got);
            Collection<BinaryTimeSeriesRow> brows = got.getBinaryValues();
            assertTrue(brows == null || brows.isEmpty());  // its empty - but should it be?

            String nowStr = Instant.now().toString();
            BinaryTimeSeriesRow row = new BinaryTimeSeriesRow.Builder()
                    .withDateTime(startZDT.toInstant())
                    .withBinaryValue(nowStr.getBytes())
                    .withFilename("file.bin")
                    .withMediaType("application/octet-stream")
                    .build();
            BinaryTimeSeries bts = new BinaryTimeSeries.Builder()
                    .withOfficeId(officeId)
                    .withName(tsId)
                    .withBinaryValue(row)
                    .build();
            dao.store(bts, true, true );

            got = dao.retrieve(officeId, tsId, "*bin", startZDT.toInstant(), endZDT.toInstant(), null, true, true, null, null);
            assertNotNull(got);

            Collection<BinaryTimeSeriesRow> rows = got.getBinaryValues();
            assertNotNull(rows);
            assertFalse(rows.isEmpty());
            BinaryTimeSeriesRow firstRow = rows.iterator().next();
            assertNotNull(firstRow);
        },
        CwmsDataApiSetupCallback.getWebUser());
    }
}
