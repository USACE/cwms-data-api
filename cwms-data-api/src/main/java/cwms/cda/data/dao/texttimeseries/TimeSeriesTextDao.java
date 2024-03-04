package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import usace.cwms.db.jooq.codegen.tables.AV_TS_TEXT;

// based on https://bitbucket.hecdev.net/projects/CWMS/repos/hec-cwms-data-access/browse/hec-db-jdbc/src/main/java/wcds/dbi/oracle/cwms/CwmsTimeSeriesTextJdbcDao.java
public final class TimeSeriesTextDao extends JooqDao<TextTimeSeries> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();


    public static final String OFFICE_ID = "OFFICE_ID";


    public TimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public TextTimeSeries retrieveFromDao(
                                          @NotNull String officeId, @NotNull String tsId, String textMask,
                                          @NotNull ZonedDateTime startTime, @NotNull ZonedDateTime endTime,
                                          @Nullable ZonedDateTime versionDate,
                                          boolean maxVersion,
                                          @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {

        List<RegularTextTimeSeriesRow> regRows = null;


        RegularTimeSeriesTextDao regDao = getRegularDao();
        Instant instant = null;
        if (versionDate != null) {
            instant = versionDate.toInstant();
        }
        regRows = regDao.retrieveRows(officeId, tsId, textMask,
                startTime.toInstant(), endTime.toInstant(), instant,
                maxVersion, minAttribute, maxAttribute);

        return new TextTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withRegularTextValues(regRows)
                .build();
    }

    public TextTimeSeries retrieveFromView(@NotNull String officeId, @NotNull String tsId,
                                           @Nullable ZonedDateTime startTime, @Nullable ZonedDateTime endTime,
                                           @Nullable ZonedDateTime versionDate,
                                           @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {
        Condition conditions = AV_TS_TEXT.AV_TS_TEXT.OFFICE_ID.eq(officeId)
                .and(AV_TS_TEXT.AV_TS_TEXT.CWMS_TS_ID.eq(tsId));

        if (startTime != null) {
            conditions =
                    conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.ge(Timestamp.from(startTime.toInstant())));
        }

        if (endTime != null) {
            conditions =
                    conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.le(Timestamp.from(endTime.toInstant())));
        }

        if (versionDate != null) {
            conditions =
                    conditions.and(AV_TS_TEXT.AV_TS_TEXT.VERSION_DATE_UTC.eq(Timestamp.from(versionDate.toInstant())));
        }

        if (minAttribute != null) {
            conditions =
                    conditions.and(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE.ge(BigDecimal.valueOf(minAttribute)));
        }

        if (maxAttribute != null) {
            conditions =
                    conditions.and(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE.le(BigDecimal.valueOf(maxAttribute)));
        }

        List<TextTimeSeriesRow> rows = dsl.select(
                        AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC,
                        AV_TS_TEXT.AV_TS_TEXT.VERSION_DATE_UTC,
                        AV_TS_TEXT.AV_TS_TEXT.DATA_ENTRY_DATE_UTC,
                        AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE,
                        AV_TS_TEXT.AV_TS_TEXT.STD_TEXT_ID,
                        AV_TS_TEXT.AV_TS_TEXT.TEXT_VALUE
                )
                .from(AV_TS_TEXT.AV_TS_TEXT)
                .where(conditions)
                .stream()
                .map(r -> buildRow(r, officeId))
                .collect(Collectors.toList());

        return new TextTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withRows(rows)
                .build();
    }


    private TextTimeSeriesRow buildRow(Record next, String officeId) {

        Timestamp dateTimeUtc = next.get(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC);
        Timestamp versionDateUtc = next.get(AV_TS_TEXT.AV_TS_TEXT.VERSION_DATE_UTC);
        Timestamp dataEntryDateUtc = next.get(AV_TS_TEXT.AV_TS_TEXT.DATA_ENTRY_DATE_UTC);
        BigDecimal attribute = next.get(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE);

        String textValue = next.get(AV_TS_TEXT.AV_TS_TEXT.TEXT_VALUE);

        Long attrLong = null;
        if (attribute != null) {
            attrLong = attribute.longValue();
        }

        return RegularTimeSeriesTextDao.buildRow(dateTimeUtc, versionDateUtc, dataEntryDateUtc, attrLong, null,  textValue);
    }



    public void create(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {



        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.storeRows(tts.getOfficeId(), tts.getName(), maxVersion, replaceAll, regRows);
        }

    }
    
    public void store(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();

            for (RegularTextTimeSeriesRow regRow : regRows) {
                regDao.storeRow(tts.getOfficeId(), tts.getName(), regRow, maxVersion, replaceAll);
            }

        }

    }
    

    public void delete( String officeId, String textTimeSeriesId, String textMask,
                       @NotNull ZonedDateTime start, @NotNull ZonedDateTime end, @Nullable ZonedDateTime versionDate,
                       boolean maxVersion, Long minAttribute, Long maxAttribute) {

        Instant versionInstant = null;
        if (versionDate != null) {
            versionInstant = versionDate.toInstant();
        }

        RegularTimeSeriesTextDao regDao = getRegularDao();
        regDao.delete(officeId, textTimeSeriesId, textMask,
                start.toInstant(), end.toInstant(), versionInstant,
                maxVersion, minAttribute, maxAttribute);

    }


    @NotNull
    private RegularTimeSeriesTextDao getRegularDao() {
        return new RegularTimeSeriesTextDao(dsl);
    }

    @Nullable
    public static Date getDate(@Nullable Instant startTime) {
        Date startDate;
        if (startTime != null) {
            startDate = Date.from(startTime);
        } else {
            startDate = null;
        }
        return startDate;
    }

    @Nullable
    public static Date getDate(@Nullable Timestamp startTime) {
        Date startDate;
        if (startTime != null) {
            startDate = new Date(startTime.getTime());
        } else {
            startDate = null;
        }
        return startDate;
    }
}
