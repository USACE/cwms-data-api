package cwms.cda.data.dao.texttimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.timeseriestext.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.StandardTextId;
import cwms.cda.data.dto.timeseriestext.StandardTextTimeSeriesRow;
import cwms.cda.data.dto.timeseriestext.StandardTextValue;
import cwms.cda.data.dto.timeseriestext.TextTimeSeries;
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



    public TextTimeSeries retrieveFromView(@NotNull String officeId, @NotNull String tsId,
                                           @Nullable Date startTime, @Nullable Date endTime,
                                           @Nullable Date versionDate,
                                           @Nullable Long minAttribute, @Nullable Long maxAttribute
    ) {
        Condition conditions = AV_TS_TEXT.AV_TS_TEXT.OFFICE_ID.eq(officeId)
                .and(AV_TS_TEXT.AV_TS_TEXT.CWMS_TS_ID.eq(tsId));

        if (startTime != null) {
            conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.ge(new Timestamp(startTime.getTime())));
        }

        if (endTime != null) {
            conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC.le(new Timestamp(endTime.getTime())));
        }

        if (versionDate != null) {
            conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.VERSION_DATE_UTC.eq(new Timestamp(versionDate.getTime())));
        }

        if (minAttribute != null) {
            conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE.ge(BigDecimal.valueOf(minAttribute)));
        }

        if (maxAttribute != null) {
            conditions = conditions.and(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE.le(BigDecimal.valueOf(maxAttribute)));
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
                .withId(tsId)
                .withRows(rows)
                .build();
    }


    private TextTimeSeriesRow buildRow(Record next, String officeId) {

        Timestamp dateTimeUTC = next.get(AV_TS_TEXT.AV_TS_TEXT.DATE_TIME_UTC);
        Timestamp versionDateUTC = next.get(AV_TS_TEXT.AV_TS_TEXT.VERSION_DATE_UTC);
        Timestamp dataEntryDateUTC = next.get(AV_TS_TEXT.AV_TS_TEXT.DATA_ENTRY_DATE_UTC);
        BigDecimal attribute = next.get(AV_TS_TEXT.AV_TS_TEXT.ATTRIBUTE);
        String stdTextId = next.get(AV_TS_TEXT.AV_TS_TEXT.STD_TEXT_ID);
        String textValue = next.get(AV_TS_TEXT.AV_TS_TEXT.TEXT_VALUE);

        if (stdTextId == null) {
            RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder();
            return builder
                    .withDateTime(new Date(dateTimeUTC.getTime()))
                    .withVersionDate(new Date(versionDateUTC.getTime()))
                    .withDataEntryDate(new Date(dataEntryDateUTC.getTime()))
                    .withAttribute(attribute)
                    .withTextValue(textValue)
                    .build();
        } else {
            StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();

            StandardTextId standardTextId = new StandardTextId.Builder()
                    .withOfficeId(officeId)
                    .withId(stdTextId)
                    .build();
            if (textValue == null) {
                builder.withStandardTextId(standardTextId);
            } else {
                builder.withStandardTextValue(new StandardTextValue.Builder()
                        .withId(standardTextId)
                        .withStandardText(textValue)
                        .build());
            }

            builder
                    .withDateTime(new Date(dateTimeUTC.getTime()))
                    .withVersionDate(new Date(versionDateUTC.getTime()))
                    .withDataEntryDate(new Date(dataEntryDateUTC.getTime()))
                .withAttribute(attribute);


            return builder.build();
        }

    }

    public void create(TextTimeSeries tts, boolean failIfExists) {

        Collection<StandardTextTimeSeriesRow> stdRows = tts.getStdRows();
        if (stdRows != null) {
            StandardTimeSeriesTextDao stdDao = new StandardTimeSeriesTextDao(dsl);

            // Is there a bulk store?
            for (StandardTextTimeSeriesRow stdRow : stdRows) {
                StandardTextValue standardTextValue = stdRow.getStandardTextValue();
                stdDao.storeStandardText(standardTextValue, failIfExists);
            }
        }

        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegRows();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = new RegularTimeSeriesTextDao(dsl);

            boolean maxVersion = false;
            boolean replaceAll = false;

            for (RegularTextTimeSeriesRow regRow : regRows) {
                regDao.storeRow(tts.getOfficeId(), tts.getId(), regRow, maxVersion, replaceAll);
            }

        }

    }

    public void delete(String officeId, String textTimeSeriesId, String textMask, ZonedDateTime start,
                       ZonedDateTime end, ZonedDateTime versionDate, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {

        boolean deleteReg = true;  // how can we tell if the user wants to delete std text or reg
        // text?
        // Maybe we try to do both?
        boolean deleteStd = true;
        if (deleteReg) {
            RegularTimeSeriesTextDao regDao = new RegularTimeSeriesTextDao(dsl);
            regDao.delete(officeId, textTimeSeriesId, textMask,
                    getInstant(start), getInstant(end), getInstant(versionDate),
                    maxVersion, minAttribute, maxAttribute);
        }
        if (deleteStd) {
            StandardTimeSeriesTextDao stdDao = new StandardTimeSeriesTextDao(dsl);
            stdDao.delete(officeId, textTimeSeriesId, textMask,
                    getInstant(start), getInstant(end), getInstant(versionDate),
                    maxVersion, minAttribute, maxAttribute);
        }

    }

    @Nullable
    private static Instant getInstant(ZonedDateTime start) {
        Instant instant = null;
        if(start != null){
            instant = start.toInstant();
        }
        return instant;
    }
}
