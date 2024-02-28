package cwms.cda.data.dao.binarytimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_DOES_NOT_EXIST_ERROR_CODE;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_ID_DOES_NOT_EXIST_ERROR_CODE;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeriesRow;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.Nullable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

public class TimeSeriesBinaryDao extends JooqDao<BinaryTimeSeries> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    Function<ResultSet, BinaryTimeSeriesRow> mapper;

    public TimeSeriesBinaryDao(DSLContext dsl) {
        super(dsl);
    }

    public TimeSeriesBinaryDao(DSLContext dsl, Function<ResultSet, BinaryTimeSeriesRow> mapper) {
        super(dsl);
        this.mapper = mapper;
    }

   public static Function<ResultSet, BinaryTimeSeriesRow> usePredicate(@Nullable UnaryOperator<String> howToBuildUrl,  Predicate<ResultSet> whenToBuildUrl ){
        return rs -> {
            try {
                return buildRow(rs, howToBuildUrl, whenToBuildUrl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Timestamp createTimestamp(long date) {

        if (logger.atFinest().isEnabled()) {
            TimeZone defaultTimeZone = DEFAULT_TIME_ZONE;
            String defaultTimeZoneDisplayName =
                    " " + defaultTimeZone.getDisplayName(defaultTimeZone.inDaylightTime(new Date(date)), TimeZone.SHORT);
            TimeZone gmtTimeZone = OracleTypeMap.GMT_TIME_ZONE;
            Date convertedDate = new Date(date);
            String utcTimeZoneDisplayName =
                    " " + gmtTimeZone.getDisplayName(gmtTimeZone.inDaylightTime(convertedDate),
                            TimeZone.SHORT);
            logger.atFinest().log("Storing date: " + dateTimeFormatter.format(date) + defaultTimeZoneDisplayName
                    + " converted to UTC date: " + dateTimeFormatter.format(convertedDate) + utcTimeZoneDisplayName);
        }
        return new Timestamp(date);
    }


    public void delete(String officeId, String tsId, String binaryTypeMask,
                       ZonedDateTime startTime, ZonedDateTime endTime, ZonedDateTime versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute){
        delete(officeId, tsId, binaryTypeMask,
                startTime == null ? null : startTime.toInstant(),
                endTime == null ? null : endTime.toInstant(),
                versionInstant == null ? null : versionInstant.toInstant(),
                maxVersion, minAttribute, maxAttribute);
    }

    private void delete(String officeId, String tsId, String binaryTypeMask,
                       Instant startTime, Instant endTime, Instant versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {
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
                    maxVersion ? "T" : "F", minAttribute, maxAttribute, officeId);
        });



    }



    void store(String officeId, String tsId, byte[] binaryData, String binaryType, Instant startTime, Instant endTime, Instant versionInstant,
                      boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll, Long attribute) {

        Timestamp startStamp = startTime == null ? null : Timestamp.from(startTime);
        Timestamp endStamp = endTime == null ? null : Timestamp.from(endTime);
        Timestamp verStamp = versionInstant == null ? null : Timestamp.from(versionInstant);
        store(dsl.configuration(), officeId, tsId, binaryData, binaryType, startStamp, endStamp, verStamp, OracleTypeMap.GMT_TIME_ZONE, maxVersion, storeExisting, storeNonExisting, replaceAll, attribute);
    }

    /**
     * Store binary data to a time series. The binary data can be:
     * <ul>
     *   <li>associated with a "normal" time series with numeric values and quality codes</li>
     *   <li>associated with a text time series (base parameter = "Text")</li>
     *   <li>the contents of a binary time series (base parameter = "Binary") that contains images, documents, etc...</li>
     * </ul>
     * Unlike a "normal" time series, which can have only one value/quality pair at any time/version date combination,
     * binary and text time series can have multiple entries at each time/version date combination.  Entries are retrieved
     * in the order they are stored.
     *
     * @param configuration The database configuration to use.  It is assumed that the setOffice call has already been made.
     * @param officeId The office that owns the time series. If not specified or NULL, the session user's default office is used.
     * @param tsId The time series identifier
     * @param binaryData The binary data to store.
     * @param binaryType The data type expressed as either an internet media type (e.g. 'application/pdf') or a file extension (e.g. '.pdf')
     * @param startStamp The first (or only) time for the binary data
     * @param endStamp The last time for the binary data. If specified the binary data is associated with all times from p_start_time to p_end_time (inclusive). Times must already exist for irregular time series.
     * @param verStamp The version date for the time series.  If not specified or NULL, the minimum or maximum version date (depending on p_max_version) is used.
     * @param timeZone The time zone for p_start_time, p_end_time, and p_version_date. If not specified or NULL, the local time zone of the time series' location is used.
     * @param maxVersion A flag specifying whether to use the maximum version date if p_version_date is not specified or NULL.
     * @param storeExisting  A flag specifying whether to store the binary data for times that already exist in the specified time series. Used only for regular time series.
     * @param storeNonExisting A flag specifying whether to store the binary data for times that don't already exist in the specified time series. Used only for regular time series.
     * @param replaceAll A flag specifying whether to replace any and all existing text with the specified text
     * @param attribute  A numeric attribute that can be used for sorting or other purposes
     */
    private static void store(Configuration configuration, String officeId, String tsId, 
                              byte[] binaryData, String binaryType, 
                              Timestamp startStamp, Timestamp endStamp, Timestamp verStamp, TimeZone timeZone, 
                              boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll, 
                              Long attribute) {

        CWMS_TEXT_PACKAGE.call_STORE_TS_BINARY(
                configuration, tsId,
                binaryData, binaryType,
                startStamp, endStamp, verStamp, timeZone.getID(),
                maxVersion ? "T" : "F", storeExisting ? "T" : "F", storeNonExisting ? "T" : "F", replaceAll ? "T" : "F",
                attribute, officeId) ;
    }

    /**
     * Stores existing time series binary data to a time series. The binary data can be:
     * <ul>
     *   <li>associated with a "normal" time series with numeric values and quality codes</li>
     *   <li>associated with a text time series (base parameter = "Text")</li>
     *   <li>the contents of a binary time series (base parameter = "Binary") that contains images, documents, etc...</li>
     * </ul>
     * Unlike a "normal" time series, which can have only one value/quality pair at any time/version date combination,
     * binary and text time series can have multiple entries at each time/version date combination.  Entries are retrieved
     * in the order they are stored.
     *
     * @param configuration The database configuration to use.  It is assumed that the setOffice call has already been made.
     * @param officeId    The office that owns the time series. If not specified or NULL, the session user's default office is used.
     * @param tsId         The time series identifier
     * @param binaryId    The unique identifier for the existing time series binary data as retrieved in retrieve_ts_binary.
     * @param startStamp   The first (or only) time for the binary data
     * @param endStamp     The last time for the binary data. If specified the binary data is associated with all times from p_start_time to p_end_time (inclusive). Times must already exist for irregular time series.
     * @param verStamp The version date for the time series.  If not specified or NULL, the minimum or maximum version date (depending on p_max_version) is used.
     * @param timeZone    The time zone for p_start_time, p_end_time, and p_version_date. If not specified or NULL, the local time zone of the time series' location is used.
     * @param maxVersion  A flag specifying whether to use the maximum version date if p_version_date is not specified or NULL.
     * @param storeExisting     A flag specifying whether to store the binary data for times that already exist in the specified time series. Used only for regular time series.
     * @param storeNonExisting A flag specifying whether to store the binary data for times that don't already exist in the specified time series. Used only for regular time series.
     * @param replaceAll  A flag specifying whether to replace any and all existing text with the specified text
     * @param attribute    A numeric attribute that can be used for sorting or other purposes
     */
    private static void store(Configuration configuration, String officeId, String tsId,
                              String binaryId, 
                              Timestamp startStamp, Timestamp endStamp, Timestamp verStamp, TimeZone timeZone,
                              boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll,
                              Long attribute) {
        CWMS_TEXT_PACKAGE.call_STORE_TS_BINARY_ID(
                configuration, tsId,
                binaryId, 
                startStamp, endStamp, verStamp, timeZone.getID(),
                maxVersion ? "T" : "F", storeExisting ? "T" : "F", storeNonExisting ? "T" : "F", replaceAll ? "T" : "F",
                attribute, officeId) ;
    }

    public BinaryTimeSeries retrieve(String officeId, String tsId, String mask,
                                     Instant startTime, Instant endTime, Instant versionInstant,
                                     boolean maxVersion, boolean retrieveBinary, Long minAttribute, Long maxAttribute) {
        List<BinaryTimeSeriesRow> binRows = retrieveRows(officeId, tsId, mask, startTime, endTime, versionInstant, maxVersion, retrieveBinary, minAttribute, maxAttribute);

        return new BinaryTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withBinaryValues(binRows)
                .build();
    }

    public List<BinaryTimeSeriesRow> retrieveRowsOld(String officeId, String tsId, String mask,
                                                  Instant startTime, Instant endTime, Instant versionInstant,
                                                  boolean maxVersion, boolean retrieveBinary, Long minAttribute, Long maxAttribute) {

        TimeZone utzZone = OracleTypeMap.GMT_TIME_ZONE;

        RecordMapper<? super Record, BinaryTimeSeriesRow> mapper = new RecordMapper<Record, BinaryTimeSeriesRow>() {
            @Override
            public @Nullable BinaryTimeSeriesRow map(Record rowRecord) {

                // Is there some way to know the names and types of the fields in the record?
                ZonedDateTime dateTimeZDT = rowRecord.get("DATE_TIME", LocalDateTime.class).atZone(utzZone.toZoneId());  // this works, even without converter
                ZonedDateTime  versionDate = rowRecord.get("VERSION_DATE", LocalDateTime.class).atZone(utzZone.toZoneId());
                ZonedDateTime dataEntryDate = rowRecord.get("DATA_ENTRY_DATE",LocalDateTime.class).atZone(utzZone.toZoneId());
                String binaryId = rowRecord.get("ID", String.class);
                BigDecimal attribute = rowRecord.get("ATTRIBUTE", BigDecimal.class);
                String fileExtension = rowRecord.get("FILE_EXT", String.class);
                String mediaType = rowRecord.get("MEDIA_TYPE_ID", String.class);
                byte[] binaryData = rowRecord.get("VALUE", byte[].class);

                return new BinaryTimeSeriesRow.Builder()
                        .withDateTime(dateTimeZDT.toInstant())
                        .withDataEntryDate(dataEntryDate.toInstant())
                        .withVersionDate(versionDate.toInstant())
                        .withBinaryId(binaryId)
                        .withAttribute(attribute==null?null:attribute.longValueExact())
                        .withMediaType(mediaType)
                        .withFileExtension(fileExtension)
                        .withBinaryValue(binaryData)
                        .build();
            }
        };

        return CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_BINARY(dsl.configuration(),
                tsId, mask,
                startTime == null ? null : Timestamp.from(startTime),
                endTime == null ? null : Timestamp.from(endTime),
                versionInstant == null ? null : Timestamp.from(versionInstant),
                utzZone.getID(),
                maxVersion ? "T" : "F", retrieveBinary ? "T" : "F", minAttribute, maxAttribute, officeId)
                .map(mapper);
    }

    public List<BinaryTimeSeriesRow> retrieveRows(String officeId, String tsId, String mask,
                                                  Instant startTime, Instant endTime, Instant versionDate,
                                                  boolean maxVersion, boolean retrieveBinary, Long minAttribute, Long maxAttribute) {
        Timestamp pStartTime = Timestamp.valueOf(startTime.atZone(ZoneId.of("UTC")).toLocalDateTime());
        Timestamp pEndTime = Timestamp.valueOf(endTime.atZone(ZoneId.of("UTC")).toLocalDateTime());
        Timestamp pVersionDate;
        if(versionDate == null){
            pVersionDate = null;
        } else {
            pVersionDate = Timestamp.valueOf(versionDate.atZone(ZoneId.of("UTC")).toLocalDateTime());
        }
        String pTimeZone = "UTC";
        String pMaxVersion = OracleTypeMap.formatBool(maxVersion);


        return connectionResult(dsl, conn -> {
            // Making the call from jOOQ with something like:
            // ResultSet retrieveTsTextF = CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_TEXT_F(dsl.configuration(),...
            // No longer works b/c we want a CLOB so that we can test its size without downloading the whole
            // thing from the db.  jOOQ doesn't support getClob in its MockResultSet.
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_BINARY(?,?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsBinText(stmt, tsId, mask, pStartTime, pEndTime, pVersionDate, pTimeZone, pMaxVersion, retrieveBinary, minAttribute, maxAttribute, officeId);
                stmt.execute();
                ResultSet rs = (ResultSet) stmt.getObject(1);

                List<BinaryTimeSeriesRow> rows = new ArrayList<>();

                while (rs.next()) {
                    BinaryTimeSeriesRow row = mapper.apply(rs);
                    rows.add(row);
                }
                return rows;
            } catch (SQLException e) {
                if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                    throw new NoDataFoundException();
                } else {
                    throw new RuntimeException(e);  // TODO: wrap with something else.
                }
            }
        });
    }

    private void parameterizeRetrieveTsBinText(CallableStatement stmt, String tsId, String mask,
                Timestamp pStartTime, Timestamp pEndTime, Timestamp pVersionDate, String pTimeZone,
                String pMaxVersion, boolean retrieveBin, Long minAttribute, Long maxAttribute, String officeId) throws SQLException {
            stmt.registerOutParameter(1, oracle.jdbc.OracleTypes.CURSOR);
            stmt.setString(2, tsId);
            stmt.setString(3, mask);
            stmt.setTimestamp(4, pStartTime);
            stmt.setTimestamp(5, pEndTime);
            stmt.setTimestamp(6, pVersionDate);
            stmt.setString(7, pTimeZone);
            stmt.setString(8, pMaxVersion);
            stmt.setString(9, retrieveBin ? "T" : "F");
            if (minAttribute == null) {
                stmt.setNull(10, oracle.jdbc.OracleTypes.NUMBER);
            } else {
                stmt.setLong(10, minAttribute);
            }
            if (maxAttribute == null) {
                stmt.setNull(11, oracle.jdbc.OracleTypes.NUMBER);
            } else {
                stmt.setLong(11, maxAttribute);
            }
            stmt.setString(12, officeId);
        }

    public void store(BinaryTimeSeries tts, boolean maxVersion,  boolean replaceAll) {
        store(tts, maxVersion, true, true, replaceAll);
    }

    public void store(BinaryTimeSeries tts, boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll) {

        if(hasRowWithIdAndValue(tts)){
            throw new IllegalArgumentException("The provided BinaryTimeSeries has an entry with a non-null binary-id and "
                    + "also a non-null binary-value.  For storage and creation either specify the id or the the value but not both.");
        }

        storeRows(tts.getOfficeId(), tts.getName(), tts.getBinaryValues(), maxVersion, storeExisting, storeNonExisting, replaceAll);
    }

    private void storeRows(String officeId, String tsId, Collection<BinaryTimeSeriesRow> rows,
                          boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll) {
        connection(dsl, connection -> {
            DSLContext connDsl = getDslContext(connection, officeId);
            connDsl.transaction((Configuration trx) -> {
                        Configuration config = trx.dsl().configuration();
                        for (BinaryTimeSeriesRow binRecord : rows) {
                            storeRow(config, officeId, tsId, binRecord, maxVersion, storeExisting, storeNonExisting, replaceAll);
                        }
                    }
                    // Implicit commit executed here
            );
        });
    }

    private void storeRow(Configuration configuration, String officeId, String tsId,
                          BinaryTimeSeriesRow binRecord,
                          boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll) {

        if(hasIdAndValue(binRecord)){
            throw new IllegalArgumentException("BinaryTimeSeriesRow cannot have both a binaryId and a binaryValue");
        }

        if(binRecord.getBinaryId() != null){
            store(configuration, officeId, tsId, binRecord.getBinaryId(),
                    Timestamp.from(binRecord.getDateTime()), Timestamp.from(binRecord.getDateTime()), Timestamp.from(binRecord.getVersionDate()), OracleTypeMap.GMT_TIME_ZONE,
                    maxVersion, storeExisting, storeNonExisting, replaceAll, binRecord.getAttribute());
        } else {
            store(configuration, officeId, tsId, binRecord.getBinaryValue(), binRecord.getMediaType(),
                    Timestamp.from(binRecord.getDateTime()), Timestamp.from(binRecord.getDateTime()), Timestamp.from(binRecord.getVersionDate()), OracleTypeMap.GMT_TIME_ZONE,
                    maxVersion, storeExisting, storeNonExisting, replaceAll, binRecord.getAttribute());
        }
    }

    private boolean hasRowWithIdAndValue(BinaryTimeSeries bts){
        boolean hasBoth = false;

        if(bts != null) {
            Collection<BinaryTimeSeriesRow> rows = bts.getBinaryValues();
            hasBoth = hasRowWithIdAndValue(rows);
        }

        return hasBoth;
    }

    private boolean hasRowWithIdAndValue(Collection<BinaryTimeSeriesRow> rows) {
        boolean hasBoth = false;
        if (rows != null) {
            for (BinaryTimeSeriesRow binRecord : rows) {
                if (hasIdAndValue(binRecord)) {
                    hasBoth = true;
                    break;
                }
            }
        }
        return hasBoth;
    }

    private boolean hasIdAndValue(BinaryTimeSeriesRow row) {
        return row != null && row.getBinaryId() != null && row.getBinaryValue() != null;
    }

    private static BinaryTimeSeriesRow buildRow(ResultSet rs, @Nullable UnaryOperator<String> mapper, @Nullable Predicate<ResultSet> shouldBuildUrl) throws SQLException {

        Instant dateTimeInstant = getInstant(rs.getTimestamp("DATE_TIME"));
        Instant versionInstant =  getInstant(rs.getTimestamp("VERSION_DATE"));
        Instant dataEntryInstant = getInstant(rs.getTimestamp("DATA_ENTRY_DATE"));

        String binaryId = rs.getString("ID");
        BigDecimal attribute = rs.getBigDecimal("ATTRIBUTE");
        if (rs.wasNull()) {
            attribute = null;
        }
        String fileExtension = rs.getString("FILE_EXT");
        String mediaType = rs.getString("MEDIA_TYPE_ID");

        byte[] binaryData = null;

        String valueUrl = null;
        if( shouldBuildUrl != null && shouldBuildUrl.test(rs)){
            valueUrl = getValueUrl(rs, mapper);
        }

        if (valueUrl == null) {
            binaryData = rs.getBytes("VALUE");
        }

        return new BinaryTimeSeriesRow.Builder()
                .withDateTime(dateTimeInstant)
                .withDataEntryDate(dataEntryInstant)
                .withVersionDate(versionInstant)
                .withBinaryId(binaryId)
                .withAttribute(attribute==null?null:attribute.longValueExact())
                .withMediaType(mediaType)
                .withFileExtension(fileExtension)
                .withBinaryValue(binaryData)
                .withUrl(valueUrl)
                .build();
    }

    @Nullable
    private static Instant getInstant(Timestamp dateTime) {
        Instant dateTimeInstant = null;
        if(dateTime != null) {
            dateTimeInstant = dateTime.toLocalDateTime().atZone(OracleTypeMap.GMT_TIME_ZONE.toZoneId()).toInstant();
        }
        return dateTimeInstant;
    }

    private static String getValueUrl(ResultSet rs, @Nullable UnaryOperator<String> howToBuildUrl) {
        String url = null;

        try {
            if(howToBuildUrl != null && rs != null) {
                String id = rs.getString("ID");

                if (id != null && ! id.isEmpty()) {
                    url = howToBuildUrl.apply(id);
                }
            }
        } catch (SQLException e) {
            logger.atWarning().withCause(e).log("Error mapping BLOB to URL");
        }

        return url;
    }

    public static Predicate<ResultSet> lengthPredicate(long byteThreshold) {
        return rs -> {
            try {
                Blob blob = rs.getBlob("VALUE");
                String blobId = rs.getString("ID");

                return (blobId != null && !blobId.isEmpty() &&
                        blob != null && blob.length() > byteThreshold);
            } catch (SQLException e) {
                logger.atWarning().withCause(e).log("Error checking BLOB length");
                return false;
            }
        };
    }

}
