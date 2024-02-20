package cwms.cda.data.dao.texttimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.getDate;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.ifc.text.CwmsDbText;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

public class RegularTimeSeriesTextDao extends JooqDao {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    public static final String TYPE = "Text Time Series";

    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String TEXT = "TEXT";
    private static final String TEXT_ID = "TEXT_ID";

    private static final String ATTRIBUTE = "ATTRIBUTE";

    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";


    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

    private static final List<String> timeSeriesTextColumnsList;
    private Predicate<ResultSet> whenToBuildUrl;

    private UnaryOperator<String>howToBuildUrl;
    private static final long LENGTH_THRESHOLD_FOR_URL_MAPPING = 255L;

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, TEXT_ID, ATTRIBUTE, TEXT};
        Arrays.sort(array);
        timeSeriesTextColumnsList = Arrays.asList(array);
    }

    public RegularTimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public RegularTimeSeriesTextDao(DSLContext dsl, @Nullable UnaryOperator<String> howToBuildUrl)
    {
        this(dsl, howToBuildUrl, lengthPredicate());
    }

    /**
     *
     * @param dsl The jOOQ DSLContext to use.
     * @param howToBuildUrl A caller provided function that will be called with the clob-id and is to return a URL to the clob-value.
     *                      If null, the URL will not be built.  The ReplaceUtils class contains helpful methods to build this operator.
     * @param whenToBuildUrl A caller provided predicate that will be called with the ResultSet and
     *                       is to return true if the URL should be built using howToBuildUrl.  If null, the URL will not be built.
     *                       The methods lengthPredicate, yesPredicate, and noPredicate are provided for convenience.
     */
    public RegularTimeSeriesTextDao(DSLContext dsl, @Nullable UnaryOperator<String> howToBuildUrl, @Nullable Predicate<ResultSet> whenToBuildUrl) {
        super(dsl);
        this.howToBuildUrl = howToBuildUrl;
        this.whenToBuildUrl = whenToBuildUrl;
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

        Timestamp pStartTime = createTimestamp(getDate(startTime));
        Timestamp pEndTime = createTimestamp(getDate(endTime));
        Timestamp pVersionDate = createTimestamp(getDate(versionDate));
        String pTimeZone = createTimeZoneId(timeZone);
        String pMaxVersion = OracleTypeMap.formatBool(maxVersion);

        rows = connectionResult(dsl, conn -> {
            // Making the call from jOOQ with something like:
            // ResultSet retrieveTsTextF = CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_TEXT_F(dsl.configuration(),...
            // No longer works b/c we want a CLOB so that we can test its size without downloading the whole
            // thing from the db.  jOOQ doesn't support getClob in its MockResultSet.
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_TEXT(?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsText(stmt, tsId, textMask, pStartTime, pEndTime, pVersionDate, pTimeZone, pMaxVersion, minAttribute, maxAttribute, officeId);
                stmt.execute();
                ResultSet rs = (ResultSet) stmt.getObject(1);

                return buildRows(rs, howToBuildUrl, whenToBuildUrl);
            } catch (SQLException e) {
                if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                    throw new NoDataFoundException();
                } else {
                    throw new RuntimeException(e);  // TODO: wrap with something else.
                }
            }
        });

        return rows;
    }

    /*
     * This method is used to parameterize the call to the stored procedure RETRIEVE_TS_TEXT*
     *    procedure retrieve_ts_text(
     *       p_cursor           out sys_refcursor,
     *       p_tsid          in     varchar2,
     *       p_text_mask     in     varchar2,
     *       p_start_time    in     date,
     *       p_end_time      in     date default null,
     *       p_version_date  in     date default null,
     *       p_time_zone     in     varchar2 default null,
     *       p_max_version   in     varchar2 default 'T',
     *       p_min_attribute in     number default null,
     *       p_max_attribute in     number default null,
     *       p_office_id     in     varchar2 default null);
     *
     */
    private static void parameterizeRetrieveTsText(CallableStatement stmt, String tsId, String textMask,
                                                   Timestamp pStartTime, Timestamp pEndTime, Timestamp pVersionDate, String pTimeZone,
                                                   String pMaxVersion, Long minAttribute, Long maxAttribute, String officeId) throws SQLException {
        stmt.registerOutParameter(1, oracle.jdbc.OracleTypes.CURSOR);
        stmt.setString(2, tsId);
        stmt.setString(3, textMask);
        stmt.setTimestamp(4, pStartTime);
        stmt.setTimestamp(5, pEndTime);
        stmt.setTimestamp(6, pVersionDate);
        stmt.setString(7, pTimeZone);
        stmt.setString(8, pMaxVersion);
        if (minAttribute == null) {
            stmt.setNull(9, oracle.jdbc.OracleTypes.NUMBER);
        } else {
            stmt.setLong(9, minAttribute);
        }
        if (maxAttribute == null) {
            stmt.setNull(10, oracle.jdbc.OracleTypes.NUMBER);
        } else {
            stmt.setLong(10, maxAttribute);
        }
        stmt.setString(11, officeId);
    }

    @NotNull
    private static List<RegularTextTimeSeriesRow> buildRows(ResultSet rs, UnaryOperator<String> howToBuildUrl, Predicate<ResultSet> whenToBuildUrl) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesTextColumnsList, TYPE);
        List<RegularTextTimeSeriesRow> rows = new ArrayList<>();

        while (rs.next()) {
            RegularTextTimeSeriesRow row = buildRow(rs, howToBuildUrl, whenToBuildUrl);
            rows.add(row);
        }
        return rows;
    }

    private static RegularTextTimeSeriesRow buildRow(ResultSet rs, @Nullable UnaryOperator<String> mapper, @Nullable Predicate<ResultSet> shouldBuildUrl) throws SQLException {

        Calendar gmtCalendar = OracleTypeMap.getInstance().getGmtCalendar();
        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME, gmtCalendar);
        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE);
        Timestamp tsDataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, gmtCalendar);
        String textId = rs.getString(TEXT_ID);
        String clobString = null;
        String valueUrl = null;


        if( shouldBuildUrl != null && shouldBuildUrl.test(rs)){
            valueUrl = getTextValueUrl(rs, mapper);
        }

        if (valueUrl == null) {
            clobString = rs.getString(TEXT);
        }

        Long attribute = rs.getLong(ATTRIBUTE);
        if (rs.wasNull()) {
            attribute = null;
        }

        return buildRow(tsDateTime, tsVersionDate, tsDataEntryDate, attribute, textId, clobString, valueUrl);
    }

    private static String getTextValueUrl(ResultSet rs, @Nullable UnaryOperator<String> howToBuildUrl) {
        String url = null;

        try {
            if(howToBuildUrl != null && rs != null) {
                String textId = rs.getString(TEXT_ID);

                if (textId != null && ! textId.isEmpty()) {
                    url = howToBuildUrl.apply(textId);
                }
            }
        } catch (SQLException e) {
            logger.atWarning().withCause(e).log("Error mapping CLOB to URL");
        }

        return url;
    }

    public static Predicate<ResultSet> lengthPredicate() {
        return lengthPredicate(LENGTH_THRESHOLD_FOR_URL_MAPPING);
    }

    public static Predicate<ResultSet> lengthPredicate(long lengthThreshold) {
        return rs -> {
            try {
                Clob clob = rs.getClob(TEXT);
                String textId = rs.getString(TEXT_ID);

                return (textId != null && !textId.isEmpty() &&
                        clob != null && clob.length() > lengthThreshold);
            } catch (SQLException e) {
                logger.atWarning().withCause(e).log("Error checking CLOB length");
                return false;
            }
        };
    }

    public static Predicate<ResultSet> yesPredicate() {
        return rs -> true;
    }
    public static Predicate<ResultSet> noPredicate() {
        return rs -> false;
    }

    public static RegularTextTimeSeriesRow buildRow(Timestamp dateTimeUtc, Timestamp versionDateUtc, Timestamp dataEntryDateUtc, Long attribute, String textId, String textValue, String url) {
        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(getDate(dateTimeUtc))
                .withVersionDate(getDate(versionDateUtc))
                .withDataEntryDate(getDate(dataEntryDateUtc))
                .withAttribute(attribute)
                .withTextId(textId)
                .withTextValue(textValue)
                .withUrl(url)
                ;

        return builder.build();
    }

    public void storeRows(String officeId, String id, boolean maxVersion, boolean replaceAll,
                          Collection<RegularTextTimeSeriesRow> regRows) {
        // This could be made into a more efficient bulk store.
        // We'd have to sort the rows by textId and textValue pairs and then build a set of all the matching dates
        // Then for each set of dates we'd call the appropriate storeTsText or storeTsTextId method.

        connection(dsl, connection -> {
            setOffice(connection, officeId);
            for (RegularTextTimeSeriesRow regRow : regRows) {
                storeRow(connection, officeId, id, regRow, maxVersion, replaceAll);
            }
        });
    }

    public void storeRow(String officeId, String tsId, RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                         boolean maxVersion, boolean replaceAll) {
        connection(dsl, connection -> {
            setOffice(connection, officeId);
            storeRow(connection, officeId, tsId, regularTextTimeSeriesRow, maxVersion, replaceAll);
        });
    }

    private void storeRow(Connection connection, String officeId, String tsId,
                          RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                          boolean maxVersion, boolean replaceAll) throws SQLException {

        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        String textId = regularTextTimeSeriesRow.getTextId();
        String textValue = regularTextTimeSeriesRow.getTextValue();
        Date dateTime = regularTextTimeSeriesRow.getDateTime();
        Date versionDate = regularTextTimeSeriesRow.getVersionDate();
        Long attribute = regularTextTimeSeriesRow.getAttribute();

        NavigableSet<Date> dates = new TreeSet<>();
        dates.add(dateTime);

        /* the pl/sql has:
            procedure store_ts_text(
                  p_tsid         in varchar2,
                  p_text         in clob,
                  p_start_time   in date,
                  p_end_time     in date default null,
                  p_version_date in date default null,
                  p_time_zone    in varchar2 default null,
                  p_max_version  in varchar2 default 'T',
                  p_existing     in varchar2 default 'T',
                  p_non_existing in varchar2 default 'F',
                  p_replace_all  in varchar2 default 'F',
                  p_attribute    in number default null,
                  p_office_id    in varchar2 default null)

             Jooq names this one:   call_STORE_TS_TEXT - takes a date range. not used here.

            and also:
                 procedure store_ts_text(
                      p_tsid         in varchar2,
                      p_text         in clob,
                      p_times        in date_table_type,
                      p_version_date in date default null,
                      p_time_zone    in varchar2 default null,
                      p_max_version  in varchar2 default 'T',
                      p_replace_all  in varchar2 default 'F',
                      p_attribute    in number default null,
                      p_office_id    in varchar2 default null)

            Jooq names this one:   call_STORE_TS_TEXT__2  - this is what we use
         */

        if(textId != null && textValue != null){
            // There are two storeTs methods.  You either:
            // 1.  store a textValue at specific times but you don't care about what the textId is.
            // 2.  make an existing textId apply at the specified times - you don't care about the current textId to textValue.
            // This branch is if the user is trying to specify the text_Id and the text_value.
            // We'll have to make some choices to implement this.
            throw new IllegalArgumentException(String.format("TextId:\"%s\" and TextValue:\"%s\" are both specified.  "
                    + "This is not supported yet.", textId, textValue));
        }

        CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);

        if (textId == null) {
            // dbText.storeTsText makes DATE_TABLE_TYPE pTimes = convertDates(dates); then calls STORE_TS_TEXT__2
            dbText.storeTsText(connection, tsId, textValue, dates,
                    versionDate, timeZone, maxVersion, replaceAll,
                    attribute, officeId);
        } else {
            // ends up calling STORE_TS_TEXT_ID__2
            dbText.storeTsTextId(connection, tsId, textId, dates,
                    versionDate, timeZone, maxVersion, replaceAll,
                    attribute, officeId);
        }
    }


    public void delete(String officeId, String tsId, String textMask,
                       Instant startTime, Instant endTime, Instant versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        connection(dsl, connection -> {
            setOffice(connection, officeId);
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);
            dbText.deleteTsText(connection, tsId, textMask, getDate(startTime), getDate(endTime),
                    getDate(versionInstant), timeZone, maxVersion, minAttribute, maxAttribute,
                    officeId);
        });
    }

}
