package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.timeSeriesText.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.timeSeriesText.StandardTextCatalog;
import cwms.cda.data.dto.timeSeriesText.StandardTextValue;
import hec.data.ITimeSeriesDescription;


import hec.data.timeSeriesText.StandardTextId;
import hec.data.timeSeriesText.TextTimeSeries;
import hec.db.DbConnection;
import hec.db.cwms.CwmsTimeSeriesTextDao;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import org.jooq.impl.DefaultBinding;
import usace.cwms.db.dao.ifc.text.CwmsDbText;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;
import wcds.dbi.oracle.CwmsDaoServiceLookup;

// based on https://bitbucket.hecdev.net/projects/CWMS/repos/hec-cwms-data-access/browse/hec-db-jdbc/src/main/java/wcds/dbi/oracle/cwms/CwmsTimeSeriesTextJdbcDao.java
public final class TextTimeSeriesDao<DeleteAction> extends JooqDao<TextTimeSeries> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    private final SimpleDateFormat _dateTimeFormatter = new SimpleDateFormat("dd-MMM-yyyy "
            + "HH:mm:ss");

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String TEXT = "TEXT";
    private static final String TEXT_ID = "TEXT_ID";
    private static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    private static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;
    private static final String ATTRIBUTE = "ATTRIBUTE";
    private static final String STD_TEXT_ID = "STD_TEXT_ID";
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";
    private static final String STD_TEXT = "STD_TEXT";

    private static List<String> timeSeriesStdTextColumnsList;
    private static List<String> stdTextCatalogColumnsList;
    private static List<String> timeSeriesTextColumnsList;

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, TEXT_ID,
                ATTRIBUTE, TEXT};
        Arrays.sort(array);
        timeSeriesTextColumnsList = Arrays.asList(array);
    }

    static {
        String[] array = new String[]{OFFICE_ID, STD_TEXT_ID, STD_TEXT};
        Arrays.sort(array);
        stdTextCatalogColumnsList = Arrays.asList(array);
    }

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, STD_TEXT_ID,
                ATTRIBUTE, STD_TEXT};
        Arrays.sort(array);
        timeSeriesStdTextColumnsList = Arrays.asList(array);
    }


    public TextTimeSeriesDao(DSLContext dsl) {
        super(dsl);
    }




    public StandardTextCatalog getCatalog(String pOfficeIdMask, String pStdTextIdMask) throws SQLException {

        CwmsTimeSeriesTextDao dbText = CwmsDaoServiceLookup.getDao(CwmsTimeSeriesTextDao.class, new DbConnection(DbConnection.DB_ORACLE));
        return connectionResult(dsl, c -> {
            hec.data.timeSeriesText.StandardTextCatalog dataCatalog = dbText.retreiveStandardTextCatalog(c, pOfficeIdMask, pStdTextIdMask);
            return new StandardTextCatalog.Builder().from(dataCatalog).build();
        });

    }

    public StandardTextCatalog getCatalogNotUsed(String pOfficeIdMask, String pStdTextIdMask) throws SQLException {

        try (ResultSet rs = CWMS_TEXT_PACKAGE.call_CAT_STD_TEXT_F(dsl.configuration(),
                pStdTextIdMask, pOfficeIdMask).intoResultSet()) {

            OracleTypeMap.checkMetaData(rs.getMetaData(), stdTextCatalogColumnsList, "Standard "
                    + "Text Catalog");
            StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
            while (rs.next()) {
                cwms.cda.data.dto.timeSeriesText.StandardTextId id = new cwms.cda.data.dto.timeSeriesText.StandardTextId.Builder()
                        .withOfficeId(rs.getString(OFFICE_ID))
                        .withId(rs.getString(STD_TEXT_ID))
                        .build();
                StandardTextValue standardTextValue = new StandardTextValue.Builder()
                        .withId(id)
                        .withStandardText(rs.getString(STD_TEXT))
                        .build();
                builder.withStandardTextValue(standardTextValue);
            }
            return builder.build();
        }

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
            logger.atFinest().log("Storing date: " + _dateTimeFormatter.format(date) + defaultTimeZoneDisplayName +
                    " converted to UTC date: " + _dateTimeFormatter.format(convertedDate) + utcTimeZoneDisplayName);
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

    public TextTimeSeries<RegularTextTimeSeriesRow> retrieveTimeSeriesText(
            ITimeSeriesDescription timeSeriesDescription, String textMask, Date startTime,
            Date endTime, Date versionDate,
            boolean maxVersion, Long minAttribute, Long maxAttribute) throws IOException {
        try {
            String pTsid = timeSeriesDescription.toString();
            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
            String officeId = timeSeriesDescription.getOfficeId();

            try (ResultSet retrieveTsTextF = retrieveTsTextF(pTsid, textMask,
                    startTime, endTime, versionDate, timeZone, maxVersion, minAttribute,
                    maxAttribute,
                    officeId)) {
                return parseTimeSeriesTextResultSet(timeSeriesDescription, retrieveTsTextF);
            }
        } catch (SQLException e) {
            if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                throw new NoDataFoundException();
            } else {
                throw new IOException(e);
            }
        }
    }

    private TextTimeSeries<RegularTextTimeSeriesRow> parseTimeSeriesTextResultSet(ITimeSeriesDescription timeSeriesDescription, ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesTextColumnsList, "Text Time "
                + "Series");

        TextTimeSeries<RegularTextTimeSeriesRow> retval = new TextTimeSeries<>(timeSeriesDescription);
        while (rs.next()) {
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
            retval.add(builder.build());
        }
        return retval;
    }







    protected void setThreadTimeZone(String timeZone) {
        if (timeZone == null) {
            timeZone = System.getProperty("cwms.default.timezone");
            if (timeZone == null) {
                timeZone = "UTC";
            }
        }
        TimeZone calTimeZone = TimeZone.getTimeZone(timeZone);
        DefaultBinding.THREAD_LOCAL.set(Calendar.getInstance(calTimeZone));
    }


    protected void setThreadTimeZone(TimeZone timeZone) {
        if (timeZone == null) {
            timeZone = TimeZone.getTimeZone("UTC");
        }
        String defaultTimeZone = System.getProperty("cwms.default.timezone");
        if (defaultTimeZone != null) {
            timeZone = TimeZone.getTimeZone(defaultTimeZone);
        }
        DefaultBinding.THREAD_LOCAL.set(Calendar.getInstance(timeZone));
    }


    public void deleteStandardText(StandardTextId standardTextId, DeleteAction deleteAction) {
        String stdTextId = standardTextId.getStandardTextId();
        String deleteActionString = deleteAction.toString();
        String officeId = standardTextId.getOfficeId();

        connection(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            try {
                dbText.deleteStdText(c, stdTextId, deleteActionString, officeId);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        });
    }


    public StandardTextValue retrieveStandardText(StandardTextId standardTextId) throws IOException {

        String stdTextId = standardTextId.getStandardTextId();
        String officeId = standardTextId.getOfficeId();

        return connectionResult(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);

            try {
                String stdTextClob = dbText.retrieveStdTextF(c, stdTextId, officeId);

                cwms.cda.data.dto.timeSeriesText.StandardTextId id = new cwms.cda.data.dto.timeSeriesText.StandardTextId.Builder()
                        .from(standardTextId)
                        .build();
                return new StandardTextValue.Builder()
                        .withId(id)
                        .withStandardText(stdTextClob)
                        .build();
            } catch (SQLException e) {
                if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE) {
                    throw new NotFoundException(e);
                } else {
                    throw new IOException(e);
                }
            }

        });


    }


    public void storeStandardText(StandardTextValue standardTextValue,
                                  boolean failIfExists) {

        cwms.cda.data.dto.timeSeriesText.StandardTextId standardTextId = standardTextValue.getId();
        String stdTextId = standardTextId.getId();
        String stdText = standardTextValue.getStandardText();
        String officeId = standardTextId.getOfficeId();

        connection(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);

            try {
                dbText.storeStdText(c, stdTextId, stdText, failIfExists, officeId);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        });
    }





    public void deleteTimeSeriesStandardText(
                                             ITimeSeriesDescription timeSeriesDescription,
                                             StandardTextId standardTextId, Date startTime,
                                             Date endTime, Date versionDate, boolean maxVersion,
                                             Long minAttribute, Long maxAttribute)  {

            String tsid = timeSeriesDescription.toString();
            String stdTextIdMask = standardTextId.getStandardTextId();
            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
            String officeId = timeSeriesDescription.getOfficeId();

            connection(dsl, c -> {
                try {
                    CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
                    dbText.deleteTsStdText(c, tsid, stdTextIdMask, startTime,
                            endTime, versionDate, timeZone, maxVersion, minAttribute,
                            maxAttribute, officeId);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            });
    }


//    public TextTimeSeries<StandardTextTimeSeriesRow> retrieveTimeSeriesStandardText(
//                                                                                    ITimeSeriesDescription timeSeriesDescription, StandardTextId standardTextId, Date startTime, Date endTime,
//                                                                                    Date versionDate, boolean maxVersion, boolean retrieveText, Long minAttribute, Long maxAttribute)
//            {
//
//            String tsid = timeSeriesDescription.toString();
//            String stdTextIdMask = null;
//            if (standardTextId != null) {
//                stdTextIdMask = standardTextId.getStandardTextId();
//            }
//            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
//            String officeId = timeSeriesDescription.getOfficeId();
//
//                return connectionResult(dsl, c -> {
//                    CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
//
//                    try {
//                        String stdTextClob = dbText.retrieveTsStdTextF(c, tsid,
//                            stdTextIdMask, startTime, endTime, versionDate, timeZone, maxVersion,
//                            retrieveText, minAttribute, maxAttribute, officeId);
//
//            return parseTimeSeriesStandardTextResultSet(timeSeriesDescription, retrieveTsStdTextF);
//
//        } catch (SQLException e) {
//            if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
//                throw new NotFoundException(e);
//            } else {
//                throw new IOException(e);
//            }
//        }
//    });

}
