package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.timeseriestext.StandardTextCatalog;
import cwms.cda.data.dto.timeseriestext.StandardTextId;
import cwms.cda.data.dto.timeseriestext.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.StandardTextValue;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

    private static List<String> timeSeriesStdTextColumnsList;
    private static List<String> stdTextCatalogColumnsList;



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

    public StandardTimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }


    public StandardTextCatalog getStandardTextCatalog(String pOfficeIdMask, String pStdTextIdMask) throws SQLException {

        try (ResultSet rs = CWMS_TEXT_PACKAGE.call_CAT_STD_TEXT_F(dsl.configuration(),
                pStdTextIdMask, pOfficeIdMask).intoResultSet()) {

            return parseStandardTextResultSet(rs);
        }

    }

    private static StandardTextCatalog parseStandardTextResultSet(ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), stdTextCatalogColumnsList, "Standard Text "
                + "Catalog");
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        while (rs.next()) {
            builder.withValue(buildStandardTextValue(
                    rs.getString(OFFICE_ID), rs.getString(STD_TEXT_ID), rs.getString(STD_TEXT)));
        }
        return builder.build();
    }

    private static StandardTextValue buildStandardTextValue(String officeId, String txtId, String txt) {
        cwms.cda.data.dto.timeseriestext.StandardTextId id =
                new cwms.cda.data.dto.timeseriestext.StandardTextId.Builder()
                        .withOfficeId(officeId)
                        .withId(txtId)
                        .build();
        return  new StandardTextValue.Builder()
                .withId(id)
                .withStandardText(txt)
                .build();
    }


    public StandardTextValue retrieveStandardText(StandardTextId standardTextId) {

        return connectionResult(dsl, c -> retrieveStandardText(c, standardTextId));
    }

    private StandardTextValue retrieveStandardText(Connection c,  StandardTextId standardTextId) throws SQLException {

            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);

            String stdTextClob = dbText.retrieveStdTextF(c,
                    standardTextId.getId(),
                    standardTextId.getOfficeId());

            return new StandardTextValue.Builder()
                    .withId(standardTextId)
                    .withStandardText(stdTextClob)
                    .build();
    }


    public void storeStandardText(StandardTextValue standardTextValue,
                                  boolean failIfExists) {

        cwms.cda.data.dto.timeseriestext.StandardTextId standardTextId = standardTextValue.getId();
        String stdTextId = standardTextId.getId();
        String stdText = standardTextValue.getStandardText();
        String officeId = standardTextId.getOfficeId();

        connection(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            dbText.storeStdText(c, stdTextId, stdText, failIfExists, officeId);
        });
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
    public void deleteStandardText(StandardTextId standardTextId, DeleteRule deleteAction) {
        String stdTextId = standardTextId.getId();
        String deleteActionString = deleteAction.toString();
        String officeId = standardTextId.getOfficeId();

        connection(dsl, c -> {
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
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            dbText.deleteTsStdText(c, tsId, stdTextIdMask, getDate(startTime),
                    getDate(endTime), getDate(versionDate), timeZone, maxVersion, minAttribute,
                    maxAttribute, officeId);
        });
    }

    @Nullable
    private static Date getDate(Instant startTime) {
        Date startDate;
        if(startTime != null) {
            startDate = Date.from(startTime);
        } else {
            startDate = null;
        }
        return startDate;
    }


    public TextTimeSeries retrieveTimeSeriesStandardText(
            String officeId, String tsId, StandardTextId standardTextId,
            Date startTime, Date endTime,
            Date versionDate, boolean maxVersion, boolean retrieveText, Long minAttribute,
            Long maxAttribute) {


        final String stdTextIdMask;
        if (standardTextId != null) {
            stdTextIdMask = standardTextId.getId();
        } else {
            stdTextIdMask = null;
        }
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;


        return connectionResult(dsl, c -> {
            CwmsDbText dbText = CwmsDbServiceLookup.buildCwmsDb(CwmsDbText.class, c);
            ResultSet retrieveTsStdTextF = dbText.retrieveTsStdTextF(c, tsId,
                    stdTextIdMask, startTime, endTime, versionDate, timeZone, maxVersion,
                    retrieveText, minAttribute, maxAttribute, officeId);

            return parseTimeSeriesStandardTextResultSet(officeId, tsId, retrieveTsStdTextF);
        });

    }

    private TextTimeSeries parseTimeSeriesStandardTextResultSet(String officeId, String tsId, ResultSet rs) throws SQLException {
        OracleTypeMap.checkMetaData(rs.getMetaData(), timeSeriesStdTextColumnsList, TYPE);

        TextTimeSeries.Builder builder = new TextTimeSeries.Builder();
        builder.withId(tsId);
        builder.withOfficeId(officeId);

        builder.withStdRows(parseRows(officeId, rs));
        return builder.build();

    }

    @NotNull
    private static List<StandardTextTimeSeriesRow> parseRows(String officeId, ResultSet rs) throws SQLException {
        List<StandardTextTimeSeriesRow> rows = new ArrayList<>();
        while (rs.next()) {
            StandardTextTimeSeriesRow row = buildStandardTextTimeSeriesRow(rs, officeId);
            rows.add(row);
        }
        return rows;
    }

    private static StandardTextTimeSeriesRow buildStandardTextTimeSeriesRow(ResultSet rs,
                                                                            String officeId) throws SQLException {
        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();
        Timestamp tsDateTime = rs.getTimestamp(DATE_TIME,
                OracleTypeMap.getInstance().getGmtCalendar());
        if (!rs.wasNull()) {
            Date dateTime = new Date(tsDateTime.getTime());
            builder.withDateTime(dateTime);
        }

        Timestamp tsVersionDate = rs.getTimestamp(VERSION_DATE);
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
        String stdTextId = rs.getString(STD_TEXT_ID);
        StandardTextId standardTextId = null;
        if (!rs.wasNull()) {
            standardTextId =
                    new StandardTextId.Builder().withOfficeId(officeId).withId(stdTextId).build();
            builder.withStandardTextId(standardTextId);
        }
        Number attribute = rs.getLong(ATTRIBUTE);
        if (!rs.wasNull()) {
            builder.withAttribute(attribute.longValue());
        }
        String clobString = rs.getString(STD_TEXT);
        if (!rs.wasNull() && standardTextId != null) {
            StandardTextValue standardTextValue =
                    new StandardTextValue.Builder().withId(standardTextId).withStandardText(clobString).build();
            builder.withStandardTextValue(standardTextValue);
        }
        return builder.build();
    }


}
