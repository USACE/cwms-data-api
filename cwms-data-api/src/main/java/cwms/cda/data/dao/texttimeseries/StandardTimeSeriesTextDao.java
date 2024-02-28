package cwms.cda.data.dao.texttimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.LENGTH_THRESHOLD_FOR_URL_MAPPING;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_DOES_NOT_EXIST_ERROR_CODE;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.TEXT_ID_DOES_NOT_EXIST_ERROR_CODE;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.getDate;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.noPredicate;
import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.yesPredicate;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.StandardTextCatalog;
import cwms.cda.data.dto.texttimeseries.StandardTextId;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextValue;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.exception.NoDataFoundException;
import usace.cwms.db.dao.ifc.text.CwmsDbText;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

public class StandardTimeSeriesTextDao extends JooqDao {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE_ID = "OFFICE_ID";
    private static final String ATTRIBUTE = "ATTRIBUTE";
    private static final String STD_TEXT_ID = "STD_TEXT_ID";
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATE_TIME = "DATE_TIME";
    private static final String STD_TEXT = "STD_TEXT";
    public static final String TYPE = "Standard Text Time Series";
    public static final String CATALOG_TYPE = "Standard Text Catalog";

    private static final List<String> timeSeriesStdTextColumnsList;
    private static final List<String> stdTextCatalogColumnsList;



    Function<ResultSet, StandardTextTimeSeriesRow> mapper;


    static {
        String[] array = new String[]{OFFICE_ID, STD_TEXT_ID, STD_TEXT};
        Arrays.sort(array);
        stdTextCatalogColumnsList = Arrays.asList(array);
    }

    static {
        String[] array = new String[]{DATE_TIME, VERSION_DATE, DATA_ENTRY_DATE, STD_TEXT_ID, ATTRIBUTE, STD_TEXT};
        Arrays.sort(array);
        timeSeriesStdTextColumnsList = Arrays.asList(array);
    }

    public StandardTimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public StandardTimeSeriesTextDao(DSLContext dsl, Function<ResultSet, StandardTextTimeSeriesRow> mapper)
    {
        this(dsl);
        this.mapper = mapper;
    }


    public static Function<ResultSet, StandardTextTimeSeriesRow> alwaysBuildUrl(@Nullable UnaryOperator<String> howToBuildUrl){
        return rs -> {
            try {
                return buildRow(rs, howToBuildUrl, yesPredicate());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

    }

    public static Function<ResultSet, StandardTextTimeSeriesRow> neverBuildUrl(){
        return rs -> {
            try {
                return buildRow(rs, null, noPredicate());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * @param howToBuildUrl A caller provided function that will be called with the clob-id and is to return a URL to the clob-value.
     *                      If null, the URL will not be built.  The ReplaceUtils class contains helpful methods to build this operator.
     * @param whenToBuildUrl A caller provided predicate that will be called with the ResultSet and
     *                       is to return true if the URL should be built using howToBuildUrl.  If null, the URL will not be built.
     *                       The methods lengthPredicate, yesPredicate, and noPredicate are provided for convenience.
     * @return
     */
    public static Function<ResultSet, StandardTextTimeSeriesRow > usePredicate(@Nullable UnaryOperator<String> howToBuildUrl,  Predicate<ResultSet> whenToBuildUrl ){
        return rs -> {
            try {
                return buildRow(rs, howToBuildUrl, whenToBuildUrl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }


    public static Predicate<ResultSet> lengthPredicate() {
        return lengthPredicate(LENGTH_THRESHOLD_FOR_URL_MAPPING);
    }

    public static Predicate<ResultSet> lengthPredicate(long lengthThreshold) {
        return rs -> {
            try {
                Clob clob = rs.getClob(STD_TEXT);
                String textId = rs.getString(STD_TEXT_ID);

                return (textId != null && !textId.isEmpty() &&
                        clob != null && clob.length() > lengthThreshold);
            } catch (SQLException e) {
                logger.atWarning().withCause(e).log("Error checking CLOB length");
                return false;
            }
        };
    }

    public StandardTextCatalog retrieveCatalog(String officeIdMask, String stdTextIdMask) throws SQLException {

        try (ResultSet rs = CWMS_TEXT_PACKAGE.call_CAT_STD_TEXT_F(dsl.configuration(),
                stdTextIdMask, officeIdMask).intoResultSet()) {

            return buildCatalog(rs);
        }

    }

    private static StandardTextCatalog buildCatalog(ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), stdTextCatalogColumnsList, CATALOG_TYPE);
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        while (rs.next()) {
            builder.withValue(buildStandardTextValue(
                    rs.getString(OFFICE_ID), rs.getString(STD_TEXT_ID), rs.getString(STD_TEXT)));
        }
        return builder.build();
    }

    private static StandardTextValue buildStandardTextValue(String officeId, String txtId, String txt) {
        StandardTextId id = new StandardTextId.Builder()
                        .withOfficeId(officeId)
                        .withId(txtId)
                        .build();
        return new StandardTextValue.Builder()
                .withId(id)
                .withStandardText(txt)
                .build();
    }


    public StandardTextValue retrieve(StandardTextId standardTextId) {
        return connectionResult(dsl, c -> retrieve(c, standardTextId));
    }

    private StandardTextValue retrieve(Connection c, StandardTextId standardTextId) throws SQLException {
        CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);

        String stdTextClob = dbText.retrieveStdTextF(c,
                standardTextId.getId(),
                standardTextId.getOfficeId());

        return new StandardTextValue.Builder()
                .withId(standardTextId)
                .withStandardText(stdTextClob)
                .build();
    }

    /**
     * This is if you want to store a new standard text id -> value mapping.
     * @param standardTextValue The standard text value to store
     * @param failIfExists true if the store should fail if the standard text id already exists
     */
    public void store(StandardTextValue standardTextValue, boolean failIfExists) {

        connection(dsl, c -> store(c, standardTextValue, failIfExists));
    }

    private void store(Connection c, StandardTextValue standardTextValue, boolean failIfExists) throws SQLException {
        StandardTextId standardTextId = standardTextValue.getId();
        String stdTextId = standardTextId.getId();
        String stdText = standardTextValue.getStandardText();
        String officeId = standardTextId.getOfficeId();
        setOffice(c, officeId);
        CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
        dbText.storeStdText(c, stdTextId, stdText, failIfExists, officeId);
    }

    public void storeRows(String officeId, String tsId, boolean maxVersion, boolean replaceAll, Collection<StandardTextTimeSeriesRow> stdRows) {
        connection(dsl, connection -> {
            setOffice(connection, officeId);
            for (StandardTextTimeSeriesRow stdRow : stdRows) {
                store(connection, officeId, tsId, stdRow, maxVersion, replaceAll);
            }
        });
    }

    public void store(String officeId, String tsId, StandardTextTimeSeriesRow stdRow,
                      boolean maxVersion, boolean replaceAll) {
        connection(dsl, connection -> {
            setOffice(connection, officeId);
            store(connection, officeId, tsId, stdRow, maxVersion, replaceAll);
        });
    }

    private void store(Connection connection, String officeId, String tsId, StandardTextTimeSeriesRow stdRow, boolean maxVersion, boolean replaceAll) throws SQLException {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        String standardTextId = stdRow.getStandardTextId();

        Date dateTime = stdRow.getDateTime();
        Date versionDate = stdRow.getVersionDate();
        Long attribute = stdRow.getAttribute();

        NavigableSet<Date> dates = new TreeSet<>();
        dates.add(dateTime);

        CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, connection);
        dbText.storeTsStdText(connection, tsId, standardTextId, dates,
                versionDate, timeZone, maxVersion, replaceAll,
                attribute, officeId);
    }

    /**
     * Deletes standard text
     *
     * @param standardTextId The standard text identifier to delete
     * @param deleteAction Specifies what to delete.  Actions are as follows:
     *      * <table class="descr">
     *      *   <tr>
     *      *     <th class="descr">p_delete_action</th>
     *      *     <th class="descr">Action</th>
     *      *   </tr>
     *      *   <tr>
     *      *     <td class="descr">cwms_util.delete_key</td>
     *      *     <td class="descr">deletes only the standard text, and then only if it is not used in any time series</td>
     *      *   </tr>
     *      *   <tr>
     *      *     <td class="descr">cwms_util.delete_data</td>
     *      *     <td class="descr">deletes only the time series references to the standard text</td>
     *      *   </tr>
     *      *   <tr>
     *      *     <td class="descr">cwms_util.delete_all</td>
     *      *     <td class="descr">deletes the standard text and all time series references to it</td>
     *      *   </tr>
     *      * </table>
     */
    public void delete(StandardTextId standardTextId, DeleteRule deleteAction) {
        String stdTextId = standardTextId.getId();
        String deleteActionString = deleteAction.toString();
        String officeId = standardTextId.getOfficeId();

        connection(dsl, c -> {
            setOffice(c, officeId);
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            dbText.deleteStdText(c, stdTextId, deleteActionString, officeId);
        });
    }


    public void delete(
            String officeId, String tsId,
            StandardTextId standardTextId, Instant startTime,
            Instant endTime, Instant versionDate, boolean maxVersion,
            Long minAttribute, Long maxAttribute) {

        String stdTextIdMask = standardTextId.getId();  // This is what CwmsTimeSeriesTextJdbcDao does to build the mask
        delete(officeId, tsId, stdTextIdMask, startTime, endTime, versionDate, maxVersion, minAttribute, maxAttribute);
    }

    public void delete(String officeId, String tsId,
                        String stdTextIdMask, @NotNull Instant startTime,
                        @NotNull Instant endTime, Instant versionDate, boolean maxVersion,
                        Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        connection(dsl, c -> {
            setOffice(c, officeId);
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            dbText.deleteTsStdText(c, tsId, stdTextIdMask, getDate(startTime),
                    getDate(endTime), getDate(versionDate), timeZone, maxVersion, minAttribute,
                    maxAttribute, officeId);
        });
    }


    protected TextTimeSeries retrieveTextTimeSeries(
            String officeId, String tsId, StandardTextId standardTextId,
            Instant startTime, Instant endTime, Instant versionDate,
            boolean maxVersion, boolean retrieveText, Long minAttribute,
            Long maxAttribute) {

        final String stdTextIdMask;
        if (standardTextId != null) {
            stdTextIdMask = standardTextId.getId();
        } else {
            stdTextIdMask = "*";
        }
        return retrieveTextTimeSeries(officeId, tsId, stdTextIdMask, startTime, endTime, versionDate, maxVersion, retrieveText, minAttribute, maxAttribute);
    }

    public TextTimeSeries retrieveTextTimeSeries(String officeId, String tsId, String stdTextIdMask, Instant startTime, Instant endTime, Instant versionDate, boolean maxVersion, boolean retrieveText, Long minAttribute, Long maxAttribute) {

        TextTimeSeries.Builder builder = new TextTimeSeries.Builder();
        builder.withName(tsId);
        builder.withOfficeId(officeId);

        List<StandardTextTimeSeriesRow> rows = retrieveRows(officeId, tsId, stdTextIdMask, startTime, endTime, versionDate, maxVersion, retrieveText, minAttribute, maxAttribute);
        builder.withStandardTextValues(rows);
        return builder.build();
    }

    public List<StandardTextTimeSeriesRow> retrieveRows(String officeId, String tsId, String stdTextIdMask, Instant startTime, Instant endTime, Instant versionDate, boolean maxVersion, boolean retrieveText, Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
        Timestamp pStartTime = Timestamp.from(startTime);
        Timestamp pEndTime = Timestamp.from(endTime);
        String pTimeZone = createTimeZoneId(timeZone);

        Timestamp pVersionDate;
        if(versionDate != null) {
            pVersionDate = Timestamp.from(versionDate);
        } else {
            pVersionDate = null;
        }
        String pMaxVersion = OracleTypeMap.formatBool(maxVersion);

        return connectionResult(dsl, conn -> {
            // Making the call from jOOQ with something like:
            // ResultSet retrieveTsTextF = CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_STD_TEXT_F(dsl.configuration(),...
            // No longer works b/c we want a CLOB so that we can test its size without downloading the whole
            // thing from the db.  jOOQ doesn't support getClob in its MockResultSet.
            try (CallableStatement stmt = conn.prepareCall("{call CWMS_TEXT.RETRIEVE_TS_STD_TEXT(?,?,?,?,?,?,?,?,?,?,?,?)}")) {
                parameterizeRetrieveTsStdText(stmt, tsId, stdTextIdMask, pStartTime, pEndTime, pVersionDate, pTimeZone, pMaxVersion, retrieveText, minAttribute, maxAttribute, officeId);
                stmt.execute();
                ResultSet rs = (ResultSet) stmt.getObject(1);

                return buildRows(rs, mapper);
            } catch (SQLException e) {
                if (e.getErrorCode() == TEXT_DOES_NOT_EXIST_ERROR_CODE || e.getErrorCode() == TEXT_ID_DOES_NOT_EXIST_ERROR_CODE) {
                    throw new NoDataFoundException();
                } else {
                    throw new RuntimeException(e);  // TODO: wrap with something else.
                }
            }
        });
    }

    public String createTimeZoneId(TimeZone timeZone) {
        String retval = null;
        if (timeZone != null) {
            retval = timeZone.getID();
        }
        return retval;
    }


    private static void parameterizeRetrieveTsStdText(CallableStatement stmt, String tsId, String textMask,
                                                   Timestamp pStartTime, Timestamp pEndTime, Timestamp pVersionDate, String pTimeZone,
                                                   String pMaxVersion, boolean retrieveText, Long minAttribute, Long maxAttribute, String officeId) throws SQLException {
        stmt.registerOutParameter(1, oracle.jdbc.OracleTypes.CURSOR);
        stmt.setString(2, tsId);
        stmt.setString(3, textMask);
        stmt.setTimestamp(4, pStartTime);
        stmt.setTimestamp(5, pEndTime);
        stmt.setTimestamp(6, pVersionDate);
        stmt.setString(7, pTimeZone);
        stmt.setString(8, pMaxVersion);
        stmt.setString(9, retrieveText ? "T" : "F");
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

    @NotNull
    private static List<StandardTextTimeSeriesRow> buildRows(String officeId, ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesStdTextColumnsList, TYPE);
        List<StandardTextTimeSeriesRow> rows = new ArrayList<>();
        while (rs.next()) {
            StandardTextTimeSeriesRow row = buildRow(rs, officeId);
            rows.add(row);
        }
        return rows;
    }

    private static StandardTextTimeSeriesRow buildRow(ResultSet rs, String officeId) throws SQLException {
        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();
        builder.withOfficeId(officeId);

        Calendar gmtCalendar = OracleTypeMap.getInstance().getGmtCalendar();
        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME, gmtCalendar);
        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE, gmtCalendar);
        Timestamp tsDataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, gmtCalendar);
        String stdTextId = rs.getString(STD_TEXT_ID);
        String clobString = rs.getString(STD_TEXT);
        Long attribute = rs.getLong(ATTRIBUTE);
        if (rs.wasNull()) {
            attribute = null;
        }

        return buildRow(tsDateTime, tsVersionDate, tsDataEntryDate, attribute, clobString, officeId, stdTextId);
    }


    @NotNull
    private static List<StandardTextTimeSeriesRow> buildRows(ResultSet rs, Function<ResultSet, StandardTextTimeSeriesRow> mapper) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesStdTextColumnsList, TYPE);
        List<StandardTextTimeSeriesRow> rows = new ArrayList<>();

        while (rs.next()) {
            StandardTextTimeSeriesRow row = mapper.apply(rs);
            rows.add(row);
        }
        return rows;
    }

    private static StandardTextTimeSeriesRow buildRow(ResultSet rs, @Nullable UnaryOperator<String> mapper, @Nullable Predicate<ResultSet> shouldBuildUrl) throws SQLException {

        Calendar gmtCalendar = OracleTypeMap.getInstance().getGmtCalendar();
        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME, gmtCalendar);
        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE);
        Timestamp tsDataEntryDate = rs.getTimestamp(DATA_ENTRY_DATE, gmtCalendar);
        String textId = rs.getString(STD_TEXT_ID);
        String clobString = null;
        String valueUrl = null;


        if( shouldBuildUrl != null && shouldBuildUrl.test(rs)){
            valueUrl = getTextValueUrl(rs, mapper);
        }

        if (valueUrl == null) {
            clobString = rs.getString(STD_TEXT);
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
                String textId = rs.getString(STD_TEXT_ID);

                if (textId != null && ! textId.isEmpty()) {
                    url = howToBuildUrl.apply(textId);
                }
            }
        } catch (SQLException e) {
            logger.atWarning().withCause(e).log("Error mapping CLOB to URL");
        }

        return url;
    }


    public static StandardTextTimeSeriesRow buildRow(Timestamp dateTimeUtc, Timestamp versionDateUtc, Timestamp dataEntryDateUtc, Long attribute, String textValue, String officeId, String stdTextId) {
        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder()
        .withOfficeId(officeId)
        .withStandardTextId(stdTextId)
        .withTextValue(textValue)
        .withAttribute(attribute)
        .withDateTime(getDate(dateTimeUtc))
        .withVersionDate(getDate(versionDateUtc))
        .withDataEntryDate(getDate(dataEntryDateUtc));

        return builder.build();
    }


}
