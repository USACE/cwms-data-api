package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.ClobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.helpers.ReplaceUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import static java.lang.String.format;

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
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_TEXT(?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsText(stmt, tsId, textMask, startTime, endTime, versionDate, officeId);
                stmt.execute();
                List<RegularTextTimeSeriesRow> rows = new ArrayList<>();
                try(ResultSet rs = (ResultSet) stmt.getObject(1)) {
                    //UTF-16 conversion and assumes 2 bytes per character
                    long characterLimit = kiloByteLimit * 1024L / 2;
                    while (rs.next()) {
                        RegularTextTimeSeriesRow row = buildRow(rs, characterLimit, urlBuilder);
                        rows.add(row);
                    }
                }
                return rows;
            } catch (SQLException e) {
                if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                    NoDataFoundException ex = new NoDataFoundException("No data found for text timeseries: " + tsId);
                    ex.initCause(e);
                    throw ex;
                } else {
                    throw new DataAccessException("Error retrieving text time series: " + tsId, e);
                }
            }
        });
    }

    private static void parameterizeRetrieveTsText(CallableStatement stmt, String tsId, String textMask,
            Instant pStartTime, Instant pEndTime, Instant pVersionDate,
            String officeId) throws SQLException {
        stmt.registerOutParameter(1, ORACLE_CURSOR_TYPE);
        stmt.setString(2, tsId);
        stmt.setString(3, textMask);
        stmt.setTimestamp(4,Timestamp.from(pStartTime), UTC_CALENDAR);
        stmt.setTimestamp(5, Timestamp.from(pEndTime), UTC_CALENDAR);
        stmt.setTimestamp(6, pVersionDate == null ?  null : Timestamp.from(pVersionDate), UTC_CALENDAR);
        stmt.setString(7, "UTC");
        stmt.setString(8, "T");
        stmt.setNull(9, Types.NUMERIC);
        stmt.setNull(10, Types.NUMERIC);
        stmt.setString(11, officeId);
    }

    private RegularTextTimeSeriesRow buildRow(ResultSet rs, long characterLimit,
            ReplaceUtils.OperatorBuilder urlBuilder) throws SQLException, IOException {
        //Implementation will change with new CWMS schema
        //https://www.hec.usace.army.mil/confluence/display/CWMS/2024-02-29+Task2A+Text-ts+and+Binary-ts+Design
        Instant dateTime = rs.getTimestamp(DATE_TIME, UTC_CALENDAR).toInstant();
        Instant dataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, UTC_CALENDAR).toInstant();
        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(dateTime)
                .withDataEntryDate(dataEntryDate)
                .withFilename(dateTime.getEpochSecond() + ".txt")
                .withMediaType("text/plain");
        Clob clob = rs.getClob(TEXT);
        if (clob.length() > characterLimit) {
            String textId = rs.getString(TEXT_ID);
            String url = urlBuilder.build().apply(dateTime.toString())
                    //Hard-coding for now. Will be removed with schema update
                    + format("&%s=%s", Controllers.CLOB_ID, URLEncoder.encode(textId, "UTF-8"));
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

    public void storeRows(String officeId, String id, boolean replaceAll,
                          Collection<RegularTextTimeSeriesRow> regRows, Instant versionDate) {
        // This could be made into a more efficient bulk store.
        // We'd have to sort the rows by textId and textValue pairs and then build a set of all the matching dates
        // Then for each set of dates we'd call the appropriate storeTsText or storeTsTextId method.

        connection(dsl, connection -> {
            DSLContext dslContext = getDslContext(connection, officeId);
            for (RegularTextTimeSeriesRow regRow : regRows) {
                storeRow(dslContext.configuration(), officeId, id, replaceAll, regRow, versionDate);
            }
        });
    }

    private void storeRow(Configuration configuration, String officeId, String tsId, boolean replaceAll,
                          RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                          Instant versionDate) {
        String textValue = regularTextTimeSeriesRow.getTextValue();
        Instant dateTime = regularTextTimeSeriesRow.getDateTime();

        CWMS_TEXT_PACKAGE.call_STORE_TS_TEXT(configuration, tsId, textValue, Timestamp.from(dateTime), Timestamp.from(dateTime),
                versionDate == null ? null : Timestamp.from(versionDate), "UTC",
                "T", "T", "T", OracleTypeMap.formatBool(replaceAll), null, officeId);
    }

    public void delete(String officeId, String tsId, String textMask,
                       @NotNull Instant startTime, @NotNull Instant endTime, Instant versionInstant) {

        connection(dsl, connection -> {
            DSLContext dslContext = getDslContext(connection, officeId);
            CWMS_TEXT_PACKAGE.call_DELETE_TS_TEXT(dslContext.configuration(), tsId, textMask,
                    Timestamp.from(startTime),
                    Timestamp.from(endTime),
                    versionInstant == null ? null : Timestamp.from(versionInstant),
                    "UTC", "T", null,
                    null, officeId);
        });
    }

}
