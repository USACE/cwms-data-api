package cwms.cda.data.dao.texttimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.getDate;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.ClobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.TreeSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.ifc.text.CwmsDbText;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

public class RegularTimeSeriesTextDao extends JooqDao {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    public static final String TYPE = "Text Time Series";
    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String TEXT = "TEXT";
    private static final String TEXT_ID = "TEXT_ID";
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";
    private static final String CLOB = "CLOB";
    private static final String MEDIA_TYPE = "MEDIA_TYPE";
    private static final String FILENAME = "FILENAME";
    public static final  String QUALITY = "QUALITY";
    public static final  String DEST_FLAG = "DEST_FLAG";



    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

    private static final List<String> timeSeriesTextColumnsList;

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, CLOB, QUALITY, FILENAME, MEDIA_TYPE, DEST_FLAG};
        Arrays.sort(array);
        timeSeriesTextColumnsList = Arrays.asList(array);
    }


    public RegularTimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public Timestamp createTimestamp(Date date) {
        Timestamp retval = null;
        if (date != null) {
            long time = date.getTime();
            retval = createTimestamp(time);
        }
        return retval;
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

    public String createTimeZoneId(TimeZone timeZone) {
        String retval = null;
        if (timeZone != null) {
            retval = timeZone.getID();
        }
        return retval;
    }


    private ResultSet retrieveTsTextF(String pTsid, String textMask,
                                     Date startTime, Date endTime, Date versionDate,
                                     TimeZone timeZone, boolean maxVersion,
                                     Long minAttribute, Long maxAttribute, String officeId) {
        Timestamp pStartTime = createTimestamp(startTime);
        Timestamp pEndTime = createTimestamp(endTime);
        Timestamp pVersionDate = createTimestamp(versionDate);
        String pTimeZone = createTimeZoneId(timeZone);
        String pMaxVersion = OracleTypeMap.formatBool(maxVersion);
        return CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_TEXT_F(dsl.configuration(),
                pTsid, textMask,
                pStartTime,
                pEndTime,
                pVersionDate,
                pTimeZone,
                pMaxVersion, minAttribute, maxAttribute, officeId).intoResultSet();
    }


    protected TextTimeSeries retrieveTimeSeriesText(
            String officeId, String tsId, String textMask,
            Instant startTime, Instant endTime, Instant versionDate,
            boolean maxVersion, Long minAttribute, Long maxAttribute)  {

        List<RegularTextTimeSeriesRow> rows = retrieveRows(officeId, tsId, textMask,
                startTime, endTime, versionDate, maxVersion, minAttribute, maxAttribute);

        TextTimeSeries.Builder builder = new TextTimeSeries.Builder();
        return builder.withName(tsId)
                .withOfficeId(officeId)
                .withRegularTextValues(rows)
                .build();

    }

    public List<RegularTextTimeSeriesRow> retrieveRows(
            String officeId, String tsId, String textMask,
            Instant startTime, Instant endTime, Instant versionDate,
            boolean maxVersion, Long minAttribute, Long maxAttribute)  {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
        List<RegularTextTimeSeriesRow> rows;

        try (ResultSet retrieveTsTextF = retrieveTsTextF(tsId, textMask,
                getDate(startTime), getDate(endTime), getDate(versionDate), timeZone,
                maxVersion, minAttribute, maxAttribute,
                officeId)) {
            rows = buildRows(retrieveTsTextF, officeId);
        } catch (SQLException e) {
            if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                throw new NoDataFoundException();
            } else {
                throw new RuntimeException(e);  // TODO: wrap with something else.
            }
        }
        return rows;
    }

    @NotNull
    private List<RegularTextTimeSeriesRow> buildRows(ResultSet rs, String office) throws SQLException {
//        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesTextColumnsList, TYPE);
        List<RegularTextTimeSeriesRow> rows = new ArrayList<>();

        while (rs.next()) {
            RegularTextTimeSeriesRow row = buildRow(rs, office);
            rows.add(row);
        }
        return rows;
    }

    private RegularTextTimeSeriesRow buildRow(ResultSet rs, String office) throws SQLException {
        Instant tsDateTime = getInstant(rs.getTimestamp(DATE_TIME));
        Instant tsDataEntryDate = getInstant(rs.getTimestamp(DATA_ENTRY_DATE));

        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(tsDateTime)
                .withDataEntryDate(tsDataEntryDate);

        ResultSetMetaData metaData = rs.getMetaData();

        String filename = null;
        if (OracleTypeMap.containsColumnIgnoreCase(metaData, FILENAME)) {
            filename = rs.getString(FILENAME);
        }
        if (filename == null) {
            filename = tsDateTime.getEpochSecond() + ".txt";
        }
        builder = builder.withFilename(filename);

        String mediaType = null;
        if (OracleTypeMap.containsColumnIgnoreCase(metaData, MEDIA_TYPE)) {
            mediaType = rs.getString(MEDIA_TYPE);
        }
        if (mediaType == null) {
            mediaType = "text/plain";
        }
        builder = builder.withMediaType(mediaType);

        String clobString = null;
        if (OracleTypeMap.containsColumnIgnoreCase(metaData, CLOB)) {
            clobString = rs.getString(CLOB);
        } else if (OracleTypeMap.containsColumnIgnoreCase(metaData, TEXT)){
            clobString = rs.getString(TEXT);
        }
        if (clobString != null) {
            builder = builder.withTextValue(clobString);
        } else if (OracleTypeMap.containsColumnIgnoreCase(metaData, TEXT_ID)){
            String textId = rs.getString(TEXT_ID);
            if (textId != null && !textId.isEmpty()) {
                ClobDao clobDao = new ClobDao(dsl);
                RegularTextTimeSeriesRow.Builder finalBuilder = builder;
                clobDao.getByUniqueName(textId, office).ifPresent(clob -> finalBuilder.withTextValue(clob.getValue()));
            }
        }

        if (OracleTypeMap.containsColumnIgnoreCase(metaData, QUALITY)) {
            long qualityCode = rs.getLong(QUALITY);
            builder = builder.withQualityCode(qualityCode);
        }

        if (OracleTypeMap.containsColumnIgnoreCase(metaData, DEST_FLAG)) {
            int destFlag = rs.getInt(DEST_FLAG);
            builder = builder.withDestFlag(destFlag);
        }

        return builder.build();
    }

    @Nullable
    private static Instant getInstant(Timestamp dateTime) {
        Instant dateTimeInstant = null;
        if(dateTime != null) {
            dateTimeInstant = dateTime.toLocalDateTime().atZone(OracleTypeMap.GMT_TIME_ZONE.toZoneId()).toInstant();
        }
        return dateTimeInstant;
    }

    public void storeRows(String officeId, String id, boolean maxVersion, boolean replaceAll,
                          Collection<RegularTextTimeSeriesRow> regRows, Instant versionDate) {
        // This could be made into a more efficient bulk store.
        // We'd have to sort the rows by textId and textValue pairs and then build a set of all the matching dates
        // Then for each set of dates we'd call the appropriate storeTsText or storeTsTextId method.

        connection(dsl, connection -> {
            setOffice(connection, officeId);
            for (RegularTextTimeSeriesRow regRow : regRows) {
                storeRow(connection, officeId, id, regRow, maxVersion, replaceAll, versionDate);
            }
        });
    }

    public void storeRow(String officeId, String tsId, RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                         boolean maxVersion, boolean replaceAll, Instant versionDate) {
        connection(dsl, connection -> {
            setOffice(connection, officeId);
            storeRow(connection, officeId, tsId, regularTextTimeSeriesRow, maxVersion, replaceAll, versionDate);
        });
    }

    private void storeRow(Connection connection, String officeId, String tsId,
                          RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                          boolean maxVersion, boolean replaceAll, Instant versionDate) throws SQLException {

        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        String textValue = regularTextTimeSeriesRow.getTextValue();
        Instant dateTime = regularTextTimeSeriesRow.getDateTime();

        Long attribute = null; // removed field.

        NavigableSet<Date> dates = new TreeSet<>();
        dates.add(Date.from(dateTime));

        CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);

        dbText.storeTsText(connection, tsId, textValue, dates,
                versionDate == null ? null : Date.from(versionDate), timeZone, maxVersion, replaceAll,
                attribute, officeId);
    }

    public void delete(String officeId, String tsId, String textMask,
                       @NotNull Instant startTime, @NotNull Instant endTime, Instant versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {

        connection(dsl, connection -> {
            DSLContext dslContext = getDslContext(connection, officeId);
            CWMS_TEXT_PACKAGE.call_DELETE_TS_TEXT(dslContext.configuration(), tsId, textMask,
                    Timestamp.from(startTime),
                    Timestamp.from(endTime),
                    versionInstant == null ? null : Timestamp.from(versionInstant),
                    "UTC", OracleTypeMap.formatBool(maxVersion), minAttribute,
                    maxAttribute, officeId);
        });
    }

}
