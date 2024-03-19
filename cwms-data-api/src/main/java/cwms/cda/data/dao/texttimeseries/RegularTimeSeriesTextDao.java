package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.ClobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.helpers.ReplaceUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.ifc.text.CwmsDbText;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.TreeSet;

public final class RegularTimeSeriesTextDao extends JooqDao {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    public static final String TYPE = "Text Time Series";
    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String TEXT = "TEXT";  // Column should be going away
    private static final String TEXT_ID = "TEXT_ID"; // Column should be going away
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";
    private static final String CLOB = "CLOB";
    private static final String MEDIA_TYPE = "MEDIA_TYPE";
    private static final String FILENAME = "FILENAME";
    public static final  String QUALITY = "QUALITY";
    public static final  String DEST_FLAG = "DEST_FLAG";
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));



    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

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


    private ResultSet retrieveTsTextF(String pTsid, String textMask, Date startTime, Date endTime,
            Date versionDate, String officeId) {
        Timestamp pStartTime = createTimestamp(startTime);
        Timestamp pEndTime = createTimestamp(endTime);
        Timestamp pVersionDate = createTimestamp(versionDate);
        String pTimeZone = OracleTypeMap.GMT_TIME_ZONE.getID();
        String pMaxVersion = "T";
        return CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_TEXT_F(dsl.configuration(), pTsid, textMask,
                pStartTime, pEndTime, pVersionDate, pTimeZone, pMaxVersion, null, null,
                officeId).intoResultSet();
    }


    protected TextTimeSeries retrieveTimeSeriesText(
            String officeId, String tsId, String textMask,
            Instant startTime, Instant endTime, Instant versionDate,
            int kiloByteLimit, ReplaceUtils.OperatorBuilder urlBuilder)  {

        List<RegularTextTimeSeriesRow> rows = retrieveRows(officeId, tsId, textMask,
                startTime, endTime, versionDate, kiloByteLimit, urlBuilder);

        TextTimeSeries.Builder builder = new TextTimeSeries.Builder();
        return builder.withName(tsId)
                .withOfficeId(officeId)
                .withRegularTextValues(rows)
                .withVersionDate(versionDate)
                .build();
    }

    public List<RegularTextTimeSeriesRow> retrieveRows(
            String officeId, String tsId, String textMask,
            Instant startTime, Instant endTime, Instant versionDate,
            int kiloByteLimit, ReplaceUtils.OperatorBuilder urlBuilder)  {
        return connectionResult(dsl, conn -> {
            // Making the call from jOOQ package codegen does not work
            // b/c jOOQ MockResultSet eagerly loads the CLOB
            // we want to only load CLOB's under kiloByteLimit size.
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_STD_TEXT(?,?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsStdText(stmt, tsId, textMask, startTime, endTime, versionDate, officeId);
                stmt.execute();
                ResultSet rs = (ResultSet) stmt.getObject(1);
                //UTF-16 conversion and assumes 2 bytes per character
                long characterLimit = kiloByteLimit * 1024L / 2;
                List<RegularTextTimeSeriesRow> rows = new ArrayList<>();
                while (rs.next()) {
                    RegularTextTimeSeriesRow row = buildRow(rs, characterLimit, urlBuilder);
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

    private static void parameterizeRetrieveTsStdText(CallableStatement stmt, String tsId, String textMask,
            Instant pStartTime, Instant pEndTime, Instant pVersionDate,
            String officeId) throws SQLException {
        stmt.registerOutParameter(1, ORACLE_CURSOR_TYPE);
        stmt.setString(2, tsId);
        stmt.setString(3, textMask);
        stmt.setTimestamp(4,Timestamp.from(pStartTime));
        stmt.setTimestamp(5, Timestamp.from(pEndTime));
        stmt.setTimestamp(6, pVersionDate == null ?  null : Timestamp.from(pVersionDate));
        stmt.setString(7, "UTC");
        stmt.setString(8, "T");
        stmt.setString(9, "T");
        stmt.setNull(10, Types.NUMERIC);
        stmt.setNull(11, Types.NUMERIC);
        stmt.setString(12, officeId);
    }

    private RegularTextTimeSeriesRow buildRow(ResultSet rs, long characterLimit,
            ReplaceUtils.OperatorBuilder urlBuilder) throws SQLException, IOException {
        Instant dateTime = rs.getTimestamp(DATE_TIME, UTC_CALENDAR).toInstant();
        Instant dataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, UTC_CALENDAR).toInstant();

        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(dateTime)
                .withDataEntryDate(dataEntryDate)
                .withFilename(dateTime.getEpochSecond() + ".txt")
                .withMediaType("text/plain");
        String textId = rs.getString(TEXT_ID);
        Clob clob = rs.getClob(TEXT);
        if (clob.length() > characterLimit) {
            String url = urlBuilder.build().apply(dateTime.toString())
                    //Hard-coding for now. Will be removed with schema update
                    + "&text-id=" + URLEncoder.encode(textId, "UTF-8");
            builder.withValueUrl(url);
        } else {
            builder.withTextValue(ClobDao.readFully(clob));
        }
        return builder.build();
    }

    @NotNull
    public static String sanitizeFilename(@Nullable String inputName) {
        String retval = inputName == null ? "" : inputName.trim();

        // If it ends in .txt, we'll remove it.
        if (retval.endsWith(".txt")) {
            retval = retval.substring(0, retval.length() - 4);
        }

        retval = retval.replaceAll("[^a-zA-Z0-9-_.]", "_");

        // If the name is too long, truncate it.
        if (retval.length() > 250) {
            retval = retval.substring(0, 250);
        }

        // Remove trailing periods
        while (retval.endsWith(".")) {
            retval = retval.substring(0, retval.length() - 1);
        }

        retval = retval.trim();

        // If the name is empty, use a default name.
        if (retval.isEmpty()) {
            retval = "unknown";
        }

        return retval + ".txt";
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
