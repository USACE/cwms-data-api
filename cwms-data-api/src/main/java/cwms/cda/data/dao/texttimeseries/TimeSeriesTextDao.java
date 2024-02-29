package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
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

    public static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    public static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;
    public static final long LENGTH_THRESHOLD_FOR_URL_MAPPING = 255L;

    public static final String OFFICE_ID = "OFFICE_ID";


    public TimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }



    public TextTimeSeries retrieveFromDao(@NotNull TimeSeriesTextMode mode,
                                          @NotNull String officeId, @NotNull String tsId, String textMask,
                                          @NotNull ZonedDateTime startTime, @NotNull ZonedDateTime endTime,
                                          @Nullable ZonedDateTime versionDate,
                                          boolean maxVersion,
                                          @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {
        List<StandardTextTimeSeriesRow> stdRows = null;
        List<RegularTextTimeSeriesRow> regRows = null;

        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            boolean retrieveText = true;  // should this be true?
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            Instant versionInst = null;
            if (versionDate != null) {
                versionInst = versionDate.toInstant();
            }
            stdRows = stdDao.retrieveRows(officeId, tsId, textMask,
                    startTime.toInstant(), endTime.toInstant(), versionInst,
                    maxVersion, retrieveText, minAttribute, maxAttribute);
        }

        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            Instant instant = null;
            if (versionDate != null) {
                instant = versionDate.toInstant();
            }
            regRows = regDao.retrieveRows(officeId, tsId, textMask,
                    startTime.toInstant(), endTime.toInstant(), instant,
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
                                          @NotNull ZonedDateTime startTime, @NotNull ZonedDateTime endTime,
                                          @Nullable ZonedDateTime versionDate,
                                          boolean maxVersion,
                                          @Nullable Long minAttribute, @Nullable Long maxAttribute,
                                          @Nullable UnaryOperator<String> idToUrl
    ) {
        List<StandardTextTimeSeriesRow> stdRows = null;
        List<RegularTextTimeSeriesRow> regRows = null;

        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            boolean retrieveText = true;  // should this be true?
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao(idToUrl);
            stdRows = stdDao.retrieveRows(officeId, tsId, textMask,
                    startTime.toInstant(), endTime.toInstant(), (versionDate== null)?null: versionDate.toInstant(),
                    maxVersion, retrieveText, minAttribute, maxAttribute);
            // Do I need to build the std catalog thing?
            // Add a flag for that or one method that builds and one that doesn't
        }

        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao(idToUrl);
            regRows = regDao.retrieveRows(officeId, tsId, textMask,
                    startTime.toInstant(), endTime.toInstant(),  (versionDate== null)?null: versionDate.toInstant(),
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
                                           @NotNull ZonedDateTime startTime, @NotNull ZonedDateTime endTime,
                                           @Nullable ZonedDateTime versionDate,
                                           @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {
        Condition conditions = AV_TS_TEXT.AV_TS_TEXT.OFFICE_ID.eq(officeId)
                .and(AV_TS_TEXT.AV_TS_TEXT.CWMS_TS_ID.eq(tsId));

        conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.ge(Timestamp.from(startTime.toInstant())));
        conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.le(Timestamp.from(endTime.toInstant())));

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
                       @NotNull ZonedDateTime start, @NotNull ZonedDateTime end, @Nullable ZonedDateTime versionDate,
                       boolean maxVersion, Long minAttribute, Long maxAttribute) {

        Instant versionInstant = null;
        if (versionDate != null) {
            versionInstant = versionDate.toInstant();
        }

        if (Objects.equals(TimeSeriesTextMode.REGULAR, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.delete(officeId, textTimeSeriesId, textMask,
                    start.toInstant(), end.toInstant(), versionInstant,
                    maxVersion, minAttribute, maxAttribute);
        }
        if (Objects.equals(TimeSeriesTextMode.STANDARD, mode) || Objects.equals(TimeSeriesTextMode.ALL, mode)) {
            StandardTimeSeriesTextDao stdDao = getStandardTimeSeriesTextDao();
            stdDao.delete(officeId, textTimeSeriesId, textMask,
                    start.toInstant(), end.toInstant(), versionInstant,
                    maxVersion, minAttribute, maxAttribute);
        }

    }

    @NotNull
    private StandardTimeSeriesTextDao getStandardTimeSeriesTextDao() {
        return new StandardTimeSeriesTextDao(dsl);
    }

    @NotNull
    private StandardTimeSeriesTextDao getStandardTimeSeriesTextDao(UnaryOperator<String> idToUrl) {
        Function<ResultSet, StandardTextTimeSeriesRow> mapper = StandardTimeSeriesTextDao.usePredicate(idToUrl, StandardTimeSeriesTextDao.lengthPredicate());
        return new StandardTimeSeriesTextDao(dsl, mapper);
    }

    @NotNull
    private RegularTimeSeriesTextDao getRegularDao(UnaryOperator<String> idToUrl){
        Function<ResultSet, RegularTextTimeSeriesRow> mapper = RegularTimeSeriesTextDao.usePredicate(idToUrl, RegularTimeSeriesTextDao.lengthPredicate());
        return new RegularTimeSeriesTextDao(dsl, mapper);
    }

    @NotNull
    private RegularTimeSeriesTextDao getRegularDao(){
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

    public static Predicate<ResultSet> yesPredicate() {
        return rs -> true;
    }
    public static Predicate<ResultSet> noPredicate() {
        return rs -> false;
    }
}
