package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
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
    private String clobTemplate = "/clob/ignored?clob-id={clob-id}&office-id={office}";


    public TimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public TimeSeriesTextDao(DSLContext dsl, String clobTemplate) {
        super(dsl);
        this.clobTemplate = clobTemplate;
    }

    public TextTimeSeries retrieveFromDao(@NotNull TimeSeriesTextMode mode,
            @NotNull String officeId, @NotNull String tsId,
                                           String textMask,
                                           @Nullable ZonedDateTime startTime, @Nullable ZonedDateTime endTime,
                                           @Nullable ZonedDateTime versionDate,
                                           boolean maxVersion,
                                           @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {
        List<StandardTextTimeSeriesRow> stdRows = null;
        List<RegularTextTimeSeriesRow> regRows = null;

        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            boolean retrieveText = true;  // should this be true?
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdRows = stdDao.retrieveRows(officeId, tsId, textMask,
                    getInstant(startTime), getInstant(endTime), getInstant(versionDate),
                    maxVersion, retrieveText, minAttribute, maxAttribute);
            // Do I need to build the std catalog thing?
            // Add a flag for that or one method that builds and one that doesn't
        }

        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regRows = regDao.retrieveRows(officeId, tsId, textMask,
                    getInstant(startTime), getInstant(endTime), getInstant(versionDate),
                    maxVersion, minAttribute, maxAttribute);
        }

        return new TextTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withRegularTextValues(regRows)
                .withStandardTextValues(stdRows)
                .build();
    }

    public TextTimeSeries retrieveFromDao(@NotNull TimeSeriesTextMode mode,
                                          @NotNull String officeId, @NotNull String tsId,
                                          String textMask,
                                          @Nullable ZonedDateTime startTime, @Nullable ZonedDateTime endTime,
                                          @Nullable ZonedDateTime versionDate,
                                          boolean maxVersion,
                                          @Nullable Long minAttribute, @Nullable Long maxAttribute,
                                          @Nullable UnaryOperator<String> idToUrl
    ) {
        List<StandardTextTimeSeriesRow> stdRows = null;
        List<RegularTextTimeSeriesRow> regRows = null;

        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            boolean retrieveText = true;  // should this be true?
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdRows = stdDao.retrieveRows(officeId, tsId, textMask,
                    getInstant(startTime), getInstant(endTime), getInstant(versionDate),
                    maxVersion, retrieveText, minAttribute, maxAttribute);
            // Do I need to build the std catalog thing?
            // Add a flag for that or one method that builds and one that doesn't
        }

        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao(idToUrl);
            regRows = regDao.retrieveRows(officeId, tsId, textMask,
                    getInstant(startTime), getInstant(endTime), getInstant(versionDate),
                    maxVersion, minAttribute, maxAttribute);
        }

        return new TextTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withRegularTextValues(regRows)
                .withStandardTextValues(stdRows)
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
        String stdTextId = next.get(AV_TS_TEXT.AV_TS_TEXT.STD_TEXT_ID);
        String textValue = next.get(AV_TS_TEXT.AV_TS_TEXT.TEXT_VALUE);

        Long attrLong = null;
        if (attribute != null) {
            attrLong = attribute.longValue();
        }

        if (stdTextId == null) {
            return RegularTimeSeriesTextDao.buildRow(dateTimeUtc, versionDateUtc, dataEntryDateUtc, attrLong, null,  textValue, null);
        } else {
            return StandardTimeSeriesTextDao.buildRow(dateTimeUtc, versionDateUtc, dataEntryDateUtc, attrLong, textValue, officeId, stdTextId);
        }

    }



    public void create(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        if (stdRows != null) {
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdDao.storeRows(tts.getOfficeId(), tts.getName(), maxVersion, replaceAll, stdRows);
        }

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.storeRows(tts.getOfficeId(), tts.getName(), maxVersion, replaceAll, regRows);

        }

    }
    
    public void store(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStandardTextValues();
        if (stdRows != null) {
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdDao.storeRows(tts.getOfficeId(), tts.getName(), maxVersion, replaceAll, stdRows);
        }

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();

            for (RegularTextTimeSeriesRow regRow : regRows) {
                regDao.storeRow(tts.getOfficeId(), tts.getName(), regRow, maxVersion, replaceAll);
            }

        }

    }
    

    public void delete(TimeSeriesTextMode mode, String officeId, String textTimeSeriesId, String textMask,
                       ZonedDateTime start, ZonedDateTime end, ZonedDateTime versionDate,
                       boolean maxVersion, Long minAttribute, Long maxAttribute) {


        Instant startInstant = getInstant(start);
        Instant endInstant = getInstant(end);
        Instant versionInstant = getInstant(versionDate);
        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.delete(officeId, textTimeSeriesId, textMask,
                    startInstant, endInstant, versionInstant,
                    maxVersion, minAttribute, maxAttribute);
        }
        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdDao.delete(officeId, textTimeSeriesId, textMask,
                    startInstant, endInstant, versionInstant,
                    maxVersion, minAttribute, maxAttribute);
        }

    }

    @NotNull
    private StandardTimeSeriesTextDao getStandardTimeSeriesTextDao() {
        return new StandardTimeSeriesTextDao(dsl);
    }

    @NotNull
    private RegularTimeSeriesTextDao getRegularDao(UnaryOperator<String> idToUrl){

        return new RegularTimeSeriesTextDao(dsl, idToUrl, RegularTimeSeriesTextDao.lengthPredicate());
    }

    @NotNull
    private RegularTimeSeriesTextDao getRegularDao(){
        return new RegularTimeSeriesTextDao(dsl);
    }

    @Nullable
    private static Instant getInstant(@Nullable ZonedDateTime start) {
        Instant instant = null;
        if (start != null) {
            instant = start.toInstant();
        }
        return instant;
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
