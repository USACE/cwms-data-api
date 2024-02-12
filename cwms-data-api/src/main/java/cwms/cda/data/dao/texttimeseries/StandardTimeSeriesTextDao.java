package cwms.cda.data.dao.texttimeseries;

import static cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao.getDate;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.StandardTextCatalog;
import cwms.cda.data.dto.texttimeseries.StandardTextId;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextValue;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
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
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
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
                store(officeId, tsId, stdRow, maxVersion, replaceAll);
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
                        String stdTextIdMask, Instant startTime,
                        Instant endTime, Instant versionDate, boolean maxVersion,
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


    public TextTimeSeries retrieveTextTimeSeries(
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
        builder.withId(tsId);
        builder.withOfficeId(officeId);

        List<StandardTextTimeSeriesRow> rows = retrieveRows(officeId, tsId, stdTextIdMask, startTime, endTime, versionDate, maxVersion, retrieveText, minAttribute, maxAttribute);
        builder.withStandardTextValues(rows);
        return builder.build();
    }

    public List<StandardTextTimeSeriesRow> retrieveRows(String officeId, String tsId, String stdTextIdMask, Instant startTime, Instant endTime, Instant versionDate, boolean maxVersion, boolean retrieveText, Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        return connectionResult(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            ResultSet retrieveTsStdTextF = dbText.retrieveTsStdTextF(c, tsId,
                    stdTextIdMask, getDate(startTime), getDate(endTime), getDate(versionDate), timeZone, maxVersion,
                    retrieveText, minAttribute, maxAttribute, officeId);

            return buildRows(officeId, retrieveTsStdTextF);
        });
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
