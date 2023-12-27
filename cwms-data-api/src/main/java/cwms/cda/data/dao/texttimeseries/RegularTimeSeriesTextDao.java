package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.timeseriestext.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.TreeSet;
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
    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy "
            + "HH:mm:ss");

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String TEXT = "TEXT";
    private static final String TEXT_ID = "TEXT_ID";

    private static final String ATTRIBUTE = "ATTRIBUTE";

    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";


    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

    private static List<String> timeSeriesTextColumnsList;

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, TEXT_ID,
                ATTRIBUTE, TEXT};
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


    public ResultSet retrieveTsTextF(String pTsid, String textMask,
                                     Date startTime, Date endTime, Date versionDate,
                                     TimeZone timeZone, boolean maxVersion,
                                     Long minAttribute, Long maxAttribute, String officeId) throws SQLException {
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



    public TextTimeSeries retrieveTimeSeriesText(
            String officeId, String tsId, String textMask, Date startTime,
            Date endTime, Date versionDate,
            boolean maxVersion, Long minAttribute, Long maxAttribute) throws RuntimeException {
        try {

            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;


            try (ResultSet retrieveTsTextF = retrieveTsTextF(tsId, textMask,
                    startTime, endTime, versionDate, timeZone, maxVersion, minAttribute,
                    maxAttribute,
                    officeId)) {
                return parseTimeSeriesTextResultSet(officeId, tsId, retrieveTsTextF);
            }
        } catch (SQLException e) {
            if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                throw new NoDataFoundException();
            } else {
                throw new RuntimeException(e);  // TODO: wrap with something else.
            }
        }
    }

    private TextTimeSeries parseTimeSeriesTextResultSet(String officeId, String tsId, ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesTextColumnsList, TYPE);

        TextTimeSeries.Builder builder = new TextTimeSeries.Builder();
        builder.withId(tsId)
                .withOfficeId(officeId);

        while (rs.next()) {
            RegularTextTimeSeriesRow row = parseRegularRow(rs);
            builder.withRegRow(row);
        }
        return builder.build();
    }

    private static RegularTextTimeSeriesRow parseRegularRow(ResultSet rs) throws SQLException {
        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder();

        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME,
                OracleTypeMap.getInstance().getGmtCalendar());
        if (!rs.wasNull()) {
            Date dateTime = new Date(tsDateTime.getTime());
            builder.withDateTime(dateTime);
        }

        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE,
                OracleTypeMap.getInstance().getGmtCalendar());
        if (!rs.wasNull()) {
            Date versionDate = new Date(tsVersionDate.getTime());
            builder.withVersionDate(versionDate);
        }
        Timestamp tsDataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE,
                OracleTypeMap.getInstance().getGmtCalendar());
        if (!rs.wasNull()) {
            Date dataEntryDate = new Date(tsDataEntryDate.getTime());
            builder.withDataEntryDate(dataEntryDate);
        }
        String textId = rs.getString(TEXT_ID);
        if (!rs.wasNull()) {
            builder.withTextId(textId);
        }
        Number attribute = rs.getLong(ATTRIBUTE);
        if (!rs.wasNull()) {
            builder.withAttribute(attribute.longValue());
        }
        String clobString = rs.getString(TEXT);
        if (!rs.wasNull()) {
            builder.withTextValue(clobString);
        }
        return builder.build();
    }


    public void storeRow(String officeId, String tsId, RegularTextTimeSeriesRow regularTextTimeSeriesRow,
                         boolean maxVersion, boolean replaceAll) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        String textId = regularTextTimeSeriesRow.getTextId();
        String textValue = regularTextTimeSeriesRow.getTextValue();
        Date dateTime = regularTextTimeSeriesRow.getDateTime();
        Date versionDate = regularTextTimeSeriesRow.getVersionDate();
        Long attribute = regularTextTimeSeriesRow.getAttribute();

        NavigableSet<Date> dates = new TreeSet<>();
        dates.add(dateTime);

        connection(dsl, connection -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);
            if (textId == null) {
                dbText.storeTsText(connection, tsId, textValue, dates,
                        versionDate, timeZone, maxVersion, replaceAll,
                        attribute, officeId);
            } else {
                dbText.storeTsTextId(connection, tsId, textId, dates,
                        versionDate, timeZone, maxVersion, replaceAll,
                        attribute, officeId);
            }
        });
    }


    public void delete(String officeId, String tsId, String textMask,
                       Instant startTime, Instant endTime, Instant versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        connection(dsl, connection -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);
            dbText.deleteTsText(connection, tsId, textMask, getDate(startTime), getDate(endTime),
                    getDate(versionInstant), timeZone, maxVersion, minAttribute, maxAttribute,
                    officeId);
        });
    }

    @Nullable
    private static Date getDate(Instant startTime) {
        Date startDate;
        if (startTime != null) {
            startDate = Date.from(startTime);
        } else {
            startDate = null;
        }
        return startDate;
    }

}