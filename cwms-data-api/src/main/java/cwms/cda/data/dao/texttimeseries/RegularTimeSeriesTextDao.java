package cwms.cda.data.dao.texttimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.getDate;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
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
import org.jetbrains.annotations.NotNull;
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

    private static final String ATTRIBUTE = "ATTRIBUTE";

    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";


    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

    private static final List<String> timeSeriesTextColumnsList;

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, TEXT_ID, ATTRIBUTE, TEXT};
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
            rows = buildRows(retrieveTsTextF);
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
    private static List<RegularTextTimeSeriesRow> buildRows(ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesTextColumnsList, TYPE);
        List<RegularTextTimeSeriesRow> rows = new ArrayList<>();

        while (rs.next()) {
            RegularTextTimeSeriesRow row = buildRow(rs);
            rows.add(row);
        }
        return rows;
    }

    private static RegularTextTimeSeriesRow buildRow(ResultSet rs) throws SQLException {

        Calendar gmtCalendar = OracleTypeMap.getInstance().getGmtCalendar();
        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME, gmtCalendar);
        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE);
        Timestamp tsDataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, gmtCalendar);
        String textId = rs.getString(TEXT_ID);
        String clobString = rs.getString(TEXT);
        Long attribute = rs.getLong(ATTRIBUTE);
        if (rs.wasNull()) {
            attribute = null;
        }

        return buildRow(tsDateTime, tsVersionDate, tsDataEntryDate, attribute, textId, clobString);
    }

    public static RegularTextTimeSeriesRow buildRow(Timestamp dateTimeUtc, Timestamp versionDateUtc, Timestamp dataEntryDateUtc, Long attribute, String textId, String textValue) {
        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder()
                .withDateTime(getDate(dateTimeUtc))
                .withVersionDate(getDate(versionDateUtc))
                .withDataEntryDate(getDate(dataEntryDateUtc))
                .withAttribute(attribute)
                .withTextId(textId)
                .withTextValue(textValue)
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
            throw new UnsupportedOperationException(String.format("TextId:\"%s\" and TextValue:\"%s\" are both specified.  "
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
