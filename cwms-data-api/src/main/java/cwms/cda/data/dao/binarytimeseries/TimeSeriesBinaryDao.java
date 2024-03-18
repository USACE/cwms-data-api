package cwms.cda.data.dao.binarytimeseries;

import static usace.cwms.db.dao.util.OracleTypeMap.formatBool;

import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dao.BlobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeriesRow;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import kotlin.Triple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

public class TimeSeriesBinaryDao extends JooqDao<BinaryTimeSeries> {
    public static final String DATE_TIME = "DATE_TIME";
    public static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    public static final String VALUE = "VALUE"; // Old
    public static final String ID = "ID";  //Old
    private static final String BLOB = "BLOB";
    private static final String MEDIA_TYPE = "MEDIA_TYPE";
    private static final String FILENAME = "FILENAME";
    public static final  String QUALITY = "QUALITY";
    public static final  String DEST_FLAG = "DEST_FLAG";


    public TimeSeriesBinaryDao(DSLContext dsl) {
        super(dsl);
    }



    public void delete(String officeId, String tsId, String binaryTypeMask,
                   ZonedDateTime startTime, ZonedDateTime endTime, ZonedDateTime versionInstant,
                   boolean maxVersion, Long minAttribute, Long maxAttribute) {
        delete(officeId, tsId, binaryTypeMask,
                startTime == null ? null : startTime.toInstant(),
                endTime == null ? null : endTime.toInstant(),
                versionInstant == null ? null : versionInstant.toInstant(),
                maxVersion, minAttribute, maxAttribute);
    }

    private void delete(String officeId, String tsId, String binaryTypeMask,
                       Instant startTime, Instant endTime, Instant versionInstant,
                        boolean maxVersion, Long minAttribute, Long maxAttribute) {
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
                    formatBool(maxVersion), minAttribute, maxAttribute, officeId);
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
     * @param verStamp The version date for the time series.  If not specified or NULL, the minimum
     *                 or maximum version date (depending on p_max_version) is used.
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

    /**
     * Stores existing time series binary data to a time series. The binary data can be:
     * <ul>
     *   <li>associated with a "normal" time series with numeric values and quality codes</li>
     *   <li>associated with a text time series (base parameter = "Text")</li>
     *   <li>the contents of a binary time series (base parameter = "Binary") that contains images,
     *   documents, etc...</li>
     * </ul>
     * Unlike a "normal" time series, which can have only one value/quality pair at any time/version
     * date combination, binary and text time series can have multiple entries at each time/version
     * date combination. Entries are retrieved in the order they are stored.
     *
     * @param configuration The database configuration to use.  It is assumed that the setOffice
     *                      call has already been made.
     * @param officeId    The office that owns the time series. If not specified or NULL, the
     *                    session user's default office is used.
     * @param tsId         The time series identifier
     * @param binaryId    The unique identifier for the existing time series binary data as
     *                    retrieved in retrieve_ts_binary.
     * @param startStamp   The first (or only) time for the binary data
     * @param endStamp     The last time for the binary data. If specified the binary data is
     *                     associated with all times from p_start_time to p_end_time (inclusive).
     *                     Times must already exist for irregular time series.
     * @param verStamp The version date for the time series.  If not specified or NULL, the minimum
     *                 or maximum version date (depending on p_max_version) is used.
     * @param timeZone    The time zone for p_start_time, p_end_time, and p_version_date. If not
     *                    specified or NULL, the local time zone of the time series' location is
     *                    used.
     * @param maxVersion  A flag specifying whether to use the maximum version date if
     *                    p_version_date is not specified or NULL.
     * @param storeExisting     A flag specifying whether to store the binary data for times that
     *                          already exist in the specified time series. Used only for regular
     *                          time series.
     * @param storeNonExisting A flag specifying whether to store the binary data for times that
     *                         don't already exist in the specified time series. Used only for
     *                         regular time series.
     * @param replaceAll  A flag specifying whether to replace any and all existing text with the
     *                    specified text
     */
    private static void store(Configuration configuration, String officeId, String tsId,
                              String binaryId, Timestamp startStamp, Timestamp endStamp,
                              Timestamp verStamp, TimeZone timeZone, boolean maxVersion,
                              boolean storeExisting, boolean storeNonExisting, boolean replaceAll) {
        CWMS_TEXT_PACKAGE.call_STORE_TS_BINARY_ID(
                configuration, tsId,
                binaryId, 
                startStamp, endStamp, verStamp, timeZone.getID(),
                formatBool(maxVersion), formatBool(storeExisting), formatBool(storeNonExisting),
                formatBool(replaceAll), null, officeId);
    }

    public void store(BinaryTimeSeries tts, boolean maxVersion,  boolean replaceAll) {
        store(tts, maxVersion, true, true, replaceAll);
    }

    public void store(BinaryTimeSeries tts, boolean maxVersion, boolean storeExisting,
                      boolean storeNonExisting, boolean replaceAll) {

        ZonedDateTime versionDateZdt = tts.getVersionDate();
        storeRows(tts.getOfficeId(), tts.getName(), tts.getBinaryValues(), maxVersion,
                storeExisting, storeNonExisting, replaceAll,
                versionDateZdt == null ? null : versionDateZdt.toInstant());
    }


    public BinaryTimeSeries retrieve(String officeId, String tsId, String mask,
                                     @NotNull Instant startTime, @NotNull Instant endTime,
                                     Instant versionInstant, boolean maxVersion,
                                     boolean retrieveBinary, Long minAttribute, Long maxAttribute) {
        List<BinaryTimeSeriesRow> binRows = retrieveRows(officeId, tsId, mask, startTime, endTime,
                versionInstant, maxVersion, retrieveBinary, minAttribute, maxAttribute);

        VersionType versionType = getVersionType(tsId, officeId, versionInstant != null);

        return new BinaryTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withBinaryValues(binRows)
                .withDateVersionType(versionType)
                .build();
    }

    @NotNull
    private VersionType getVersionType(String names, String office, boolean dateProvided) {
        VersionType dateVersionType;

        if (!dateProvided) {
            boolean isVersioned = isVersioned(names, office);

            if (isVersioned) {
                dateVersionType = VersionType.MAX_AGGREGATE;
            } else {
                dateVersionType = VersionType.UNVERSIONED;
            }

        } else {
            dateVersionType = VersionType.SINGLE_VERSION;
        }

        return dateVersionType;
    }

    private boolean isVersioned(String names, String office) {
        return connectionResult(dsl, connection -> {
            Configuration configuration = getDslContext(connection, office).configuration();
            return OracleTypeMap.parseBool(CWMS_TS_PACKAGE.call_IS_TSID_VERSIONED(configuration,
                    names, office));
        });
    }

    public List<BinaryTimeSeriesRow> retrieveRows(String officeId, String tsId, String mask,
              @NotNull Instant startTime, @NotNull Instant endTime, Instant versionInstant,
              boolean maxVersion, boolean retrieveBinary, Long minAttribute, Long maxAttribute) {

        TimeZone utzZone = OracleTypeMap.GMT_TIME_ZONE;

        RecordMapper<? super Record, BinaryTimeSeriesRow> mapper = new RecordMapper<Record, BinaryTimeSeriesRow>() {
            @Override
            public @Nullable BinaryTimeSeriesRow map(Record rowRecord) {

                ZonedDateTime dateTimeZdt = rowRecord.get(DATE_TIME, LocalDateTime.class).atZone(utzZone.toZoneId());
                ZonedDateTime dataEntryDate = rowRecord.get(DATA_ENTRY_DATE,LocalDateTime.class).atZone(utzZone.toZoneId());

                BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder()
                        .withDateTime(dateTimeZdt.toInstant())
                        .withDataEntryDate(dataEntryDate.toInstant());

                // FILENAME, MEDIA_TYPE, BLOB, DEST_FLAG and QUALITY are new and aren't coming back from db yet...
                String filename;
                if (hasField(rowRecord, FILENAME)) {
                    filename = rowRecord.get(FILENAME, String.class);
                } else {
                    filename = buildDefaultFilename(rowRecord, officeId);
                }
                builder = builder.withFilename(filename);

                String mediaType;
                if (hasField(rowRecord, MEDIA_TYPE)) {
                    mediaType = rowRecord.get(MEDIA_TYPE, String.class);
                } else {
                    mediaType = "application/octet-stream";
                }
                builder = builder.withMediaType(mediaType);

                byte[] binaryData;
                if (hasField(rowRecord, BLOB)) {
                    binaryData = rowRecord.get(BLOB, byte[].class);
                    builder = builder.withBinaryValue(binaryData);
                } else {
                    // Check the old-style fields.
                    String binaryId = null;
                    if (hasField(rowRecord, ID)) {
                        // The record is the old style with a blob-id
                        binaryId = rowRecord.get(ID, String.class);
                    }

                    if (binaryId != null) {
                        BinaryTimeSeriesRow.Builder finalBuilder = builder;
                        Consumer<Triple<InputStream, Long, String>> tripleConsumer = triple -> {
                            InputStream is = triple.getFirst();
                            // Could check the size here and build value-url instead of value.
                            if (is != null) {
                                try {
                                    byte[] bytes = BlobDao.readFully(is);
                                    finalBuilder.withBinaryValue(bytes);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        };

                        BlobDao blobDao = new BlobDao(dsl);
                        blobDao.getBlob(binaryId, officeId, tripleConsumer);
                    } else if (hasField(rowRecord, VALUE)) {
                        binaryData = rowRecord.get(VALUE, byte[].class);
                        builder = builder.withBinaryValue(binaryData);
                    }
                }

                if (hasField(rowRecord, QUALITY)) {
                    builder = builder.withQualityCode(rowRecord.get(QUALITY, Long.class));
                }

                if (hasField(rowRecord, DEST_FLAG)) {
                    builder = builder.withDestFlag(rowRecord.get(DEST_FLAG, Integer.class));
                }

                return builder.build();
            }
        };

        return CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_BINARY(dsl.configuration(),
                tsId, mask, Timestamp.from(startTime), Timestamp.from(endTime),
                versionInstant == null ? null : Timestamp.from(versionInstant),
                utzZone.getID(), formatBool(maxVersion), formatBool(retrieveBinary),
                        minAttribute, maxAttribute, officeId)
                .map(mapper);
    }

    protected String buildDefaultFilename(Record rowRecord, String officeId) {

        ZonedDateTime dateTimeZdt = rowRecord.get(DATE_TIME, LocalDateTime.class)
                .atZone(OracleTypeMap.GMT_TIME_ZONE.toZoneId());

        return dateTimeZdt.toInstant().getEpochSecond() + ".bin";
    }

    /**
     * Check if a field exists in a record
     * @param rowRecord the record to search
     * @param name the name of the field to search for
     * @return true if the field exists, false otherwise
     * @deprecated - Method should be removed once the database changes are made.  We should know
     *               what fields are expected and throw exceptions if they are not found.
     *
     */
    @Deprecated
    private boolean hasField(Record rowRecord, String name) {
        boolean retval = false;

        if (rowRecord != null) {
            retval = rowRecord.field(name) != null;
        }

        return retval;
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

    private void storeRow(String officeId, String tsId, BinaryTimeSeriesRow binRecord,
                          boolean maxVersion, boolean storeExisting, boolean storeNonExisting,
                          boolean replaceAll, Instant versionDate) {
        dsl.connection(connection -> {
            DSLContext connDsl = getDslContext(connection, officeId);
            storeRow(connDsl.configuration(), officeId, tsId, binRecord, maxVersion, storeExisting,
                    storeNonExisting, replaceAll, versionDate);
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