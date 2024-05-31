package cwms.cda.data.dao.binarytimeseries;

import cwms.cda.api.Controllers;
import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dao.BlobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeriesRow;
import cwms.cda.helpers.ReplaceUtils;
import org.jetbrains.annotations.NotNull;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_DOES_NOT_EXIST_ERROR_CODE;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_ID_DOES_NOT_EXIST_ERROR_CODE;
import static java.lang.String.format;
import static usace.cwms.db.dao.util.OracleTypeMap.formatBool;

public final class TimeSeriesBinaryDao extends JooqDao<BinaryTimeSeries> {
    private static final String DATE_TIME = "DATE_TIME";
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VALUE = "VALUE"; // Old
    private static final String ID = "ID";  //Old
    private static final String BLOB = "BLOB";
    private static final String MEDIA_TYPE = "MEDIA_TYPE_ID";
    private static final String FILENAME = "FILENAME";
    private static final  String QUALITY = "QUALITY";
    private static final  String DEST_FLAG = "DEST_FLAG";
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));


    public TimeSeriesBinaryDao(DSLContext dsl) {
        super(dsl);
    }

    public void delete(String officeId, String tsId, String binaryTypeMask,
                       Instant startTime, Instant endTime, Instant versionInstant) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        connection(dsl, connection -> {
            Configuration configuration = getDslContext(connection, officeId).configuration();
            CWMS_TEXT_PACKAGE.call_DELETE_TS_BINARY(
                    configuration,
                    tsId, binaryTypeMask,
                    startTime == null ? null : Timestamp.from(startTime),
                    endTime == null ? null : Timestamp.from(endTime),
                    versionInstant == null ? null : Timestamp.from(versionInstant),
                    timeZone.getID(),
                    "T", null, null, officeId);
        });
    }

    void store(String officeId, String tsId, byte[] binaryData, String binaryType,
               Instant startTime, Instant endTime, Instant versionInstant, boolean maxVersion,
               boolean storeExisting, boolean storeNonExisting, boolean replaceAll) {

        Timestamp startStamp = startTime == null ? null : Timestamp.from(startTime);
        Timestamp endStamp = endTime == null ? null : Timestamp.from(endTime);
        Timestamp verStamp = versionInstant == null ? null : Timestamp.from(versionInstant);
        store(dsl.configuration(), officeId, tsId, binaryData, binaryType,
                startStamp, endStamp, verStamp, OracleTypeMap.GMT_TIME_ZONE,
                maxVersion, storeExisting, storeNonExisting, replaceAll);
    }

    /**
     * Store binary data to a time series. The binary data can be:
     * <ul>
     *   <li>associated with a "normal" time series with numeric values and quality codes</li>
     *   <li>associated with a text time series (base parameter = "Text")</li>
     *   <li>the contents of a binary time series (base parameter = "Binary") that contains
     *   images, documents, etc...</li>
     * </ul>
     * Unlike a "normal" time series, which can have only one value/quality pair at any time/version
     * date combination, binary and text time series can have multiple entries at each time/version
     * date combination.  Entries are retrieved in the order they are stored.
     *
     * @param configuration The database configuration to use.  It is assumed that the setOffice
     *                      call has already been made.
     * @param officeId The office that owns the time series. If not specified or NULL, the session
     *                 user's default office is used.
     * @param tsId The time series identifier
     * @param binaryData The binary data to store.
     * @param binaryType The data type expressed as either an internet media type
     *                   (e.g. 'application/pdf') or a file extension (e.g. '.pdf')
     * @param startStamp The first (or only) time for the binary data
     * @param endStamp The last time for the binary data. If specified the binary data is
     *                 associated with all times from p_start_time to p_end_time (inclusive). Times
     *                 must already exist for irregular time series.
     * @param verStamp The version date for the time series.  If not specified or NULL, the
     *                 maximum version date is used.
     * @param timeZone The time zone for p_start_time, p_end_time, and p_version_date. If not
     *                 specified or NULL, the local time zone of the time series' location is used.
     * @param maxVersion A flag specifying whether to use the maximum version date if
     *                   p_version_date is not specified or NULL.
     * @param storeExisting  A flag specifying whether to store the binary data for times that
     *                       already exist in the specified time series. Used only for regular
     *                       time series.
     * @param storeNonExisting A flag specifying whether to store the binary data for times that
     *                         don't already exist in the specified time series. Used only for
     *                         regular time series.
     * @param replaceAll A flag specifying whether to replace any and all existing text with the
     *                   specified text

     */
    private static void store(Configuration configuration, String officeId, String tsId, 
                              byte[] binaryData, String binaryType, Timestamp startStamp,
                              Timestamp endStamp, Timestamp verStamp, TimeZone timeZone,
                              boolean maxVersion, boolean storeExisting, boolean storeNonExisting,
                              boolean replaceAll) {

        CWMS_TEXT_PACKAGE.call_STORE_TS_BINARY(configuration, tsId, binaryData, binaryType,
                startStamp, endStamp, verStamp, timeZone.getID(), formatBool(maxVersion),
                formatBool(storeExisting), formatBool(storeNonExisting), formatBool(replaceAll),
                null, officeId);
    }

    public void store(BinaryTimeSeries tts, boolean maxVersion,  boolean replaceAll) {
        store(tts, maxVersion, true, true, replaceAll);
    }

    public void store(BinaryTimeSeries tts, boolean maxVersion, boolean storeExisting,
                      boolean storeNonExisting, boolean replaceAll) {

        Instant versionDateZdt = tts.getVersionDate();
        storeRows(tts.getOfficeId(), tts.getName(), tts.getBinaryValues(), maxVersion,
                storeExisting, storeNonExisting, replaceAll,
                versionDateZdt);
    }


    public BinaryTimeSeries retrieve(String officeId, String tsId, String mask,
            @NotNull Instant startTime, @NotNull Instant endTime,
            Instant versionInstant, int kiloByteLimit, ReplaceUtils.OperatorBuilder urlBuilder) {
        List<BinaryTimeSeriesRow> binRows = retrieveRows(officeId, tsId, mask, startTime, endTime,
                versionInstant, kiloByteLimit, urlBuilder);

        VersionType versionType = TimeSeriesDaoImpl.getVersionType(dsl, tsId, officeId, versionInstant != null);
        String timeZoneId = TimeSeriesDaoImpl.getTimeZoneId(dsl, tsId, officeId);
        return new BinaryTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withBinaryValues(binRows)
                .withDateVersionType(versionType)
                .withVersionDate(versionInstant)
                .withTimeZone(timeZoneId)
                .build();
    }

    public List<BinaryTimeSeriesRow> retrieveRows(String officeId, String tsId, String mask,
            @NotNull Instant startTime, @NotNull Instant endTime, Instant versionInstant,
            int kiloByteLimit, ReplaceUtils.OperatorBuilder urlBuilder) {
        return connectionResult(dsl, conn -> {
            // Making the call from jOOQ package codegen does not work
            // b/c jOOQ MockResultSet eagerly loads the BLOB
            // we want to only load BLOB's under kiloByteLimit size.
            Timestamp pStartTime = Timestamp.from(startTime);
            Timestamp pEndTime = Timestamp.from(endTime);
            Timestamp pVersionDate = versionInstant == null ? null : Timestamp.from(versionInstant);
            String pTimeZone = "UTC";

            long byteLimit = kiloByteLimit * 1024L;
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_BINARY(?,?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsBinText(stmt, tsId, mask, pStartTime, pEndTime, pVersionDate, pTimeZone, officeId);
                stmt.execute();
                List<BinaryTimeSeriesRow> rows = new ArrayList<>();
                try(ResultSet rs = (ResultSet) stmt.getObject(1)) {
                    while (rs.next()) {
                        BinaryTimeSeriesRow row = buildRow(byteLimit, urlBuilder, rs);
                        rows.add(row);
                    }
                }
                return rows;
            } catch (SQLException e) {
                int errorCode = e.getErrorCode();
                if (errorCode == TEXT_DOES_NOT_EXIST_ERROR_CODE || errorCode == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                    NoDataFoundException ex = new NoDataFoundException("No data found for binary timeseries: " + tsId);
                    ex.initCause(e);
                    throw ex;
                } else {
                    throw new DataAccessException("Error retrieving binary timeseries: " + tsId, e);
                }
            }
        });
    }

    private BinaryTimeSeriesRow buildRow(long byteLimit, ReplaceUtils.OperatorBuilder urlBuilder, ResultSet rs)
            throws SQLException, IOException {
        //Implementation will change with new CWMS schema
        //https://www.hec.usace.army.mil/confluence/display/CWMS/2024-02-29+Task2A+Text-ts+and+Binary-ts+Design
        Instant dateTime = rs.getTimestamp(DATE_TIME, UTC_CALENDAR).toInstant();
        Instant dataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, UTC_CALENDAR).toInstant();
        String mediaType = rs.getString(MEDIA_TYPE);
        BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder()
                .withDateTime(dateTime)
                .withDataEntryDate(dataEntryDate)
                .withFilename(dateTime.getEpochSecond() + ".bin")
                .withMediaType(mediaType)
                .withQualityCode(0L)
                .withDestFlag(0);
        Blob b = rs.getBlob(VALUE);
        if (b.length() > byteLimit) {
            String binaryId = rs.getString(ID);
            String url = urlBuilder.build().apply(dateTime.toString())
                    //Hard-coding for now. Will be removed with schema update
                    + format("&%s=%s", Controllers.BLOB_ID, URLEncoder.encode(binaryId, "UTF-8"));
            builder.withValueUrl(url);
        } else {
            try (InputStream is = b.getBinaryStream()) {
                byte[] bytes = BlobDao.readFully(is);
                builder.withBinaryValue(bytes);
            }
        }
        return builder.build();
    }

    private void parameterizeRetrieveTsBinText(CallableStatement stmt, String tsId, String mask,
            Timestamp pStartTime, Timestamp pEndTime, Timestamp pVersionDate, String pTimeZone,
            String officeId) throws SQLException {
        stmt.registerOutParameter(1, ORACLE_CURSOR_TYPE);
        stmt.setString(2, tsId);
        stmt.setString(3, mask);
        stmt.setTimestamp(4, pStartTime, UTC_CALENDAR);
        stmt.setTimestamp(5, pEndTime, UTC_CALENDAR);
        stmt.setTimestamp(6, pVersionDate, UTC_CALENDAR);
        stmt.setString(7, pTimeZone);
        stmt.setString(8, "T");
        stmt.setString(9, "T");
        stmt.setNull(10, Types.NUMERIC);
        stmt.setNull(11, Types.NUMERIC);
        stmt.setString(12, officeId);
    }

    private void storeRows(String officeId, String tsId, Collection<BinaryTimeSeriesRow> rows,
                          boolean maxVersion, boolean storeExisting, boolean storeNonExisting,
                           boolean replaceAll, Instant versionDate) {
        dsl.connection(connection -> {
            DSLContext connDsl = getDslContext(connection, officeId);
            connDsl.transaction((Configuration trx) -> {
                Configuration config = trx.dsl().configuration();
                for (BinaryTimeSeriesRow binRecord : rows) {
                    storeRow(config, officeId, tsId, binRecord, maxVersion, storeExisting,
                            storeNonExisting, replaceAll, versionDate);
                }
            });
        });
    }

    private void storeRow(Configuration configuration, String officeId, String tsId,
                          BinaryTimeSeriesRow binRecord,
                          boolean maxVersion, boolean storeExisting, boolean storeNonExisting,
                          boolean replaceAll, Instant versionDate) {
        Instant dateTime = binRecord.getDateTime();
        Timestamp dateTimestamp = dateTime == null ? null : Timestamp.from(dateTime);
        Timestamp versionStamp = versionDate == null ? null : Timestamp.from(versionDate);
        store(configuration, officeId, tsId, binRecord.getBinaryValue(), binRecord.getMediaType(),
                dateTimestamp, dateTimestamp, versionStamp, OracleTypeMap.GMT_TIME_ZONE,
                maxVersion, storeExisting, storeNonExisting, replaceAll);
    }

}