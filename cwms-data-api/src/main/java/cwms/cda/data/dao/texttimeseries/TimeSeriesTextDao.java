package cwms.cda.data.dao.texttimeseries;

import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.helpers.ReplaceUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public final class TimeSeriesTextDao extends JooqDao<TextTimeSeries> {

    public static final int TEXT_DOES_NOT_EXIST_ERROR_CODE = 20034;
    public static final int TEXT_ID_DOES_NOT_EXIST_ERROR_CODE = 20001;

    public TimeSeriesTextDao(DSLContext dsl) {
        super(dsl);
    }

    public TextTimeSeries retrieveFromDao(@NotNull String officeId, @NotNull String tsId,
            String textMask, @NotNull Instant startTime, @NotNull Instant endTime,
            @Nullable Instant versionDate, int kiloByteLimit, ReplaceUtils.OperatorBuilder urlBuilder) {
        List<RegularTextTimeSeriesRow> regRows;

        RegularTimeSeriesTextDao regDao = new RegularTimeSeriesTextDao(dsl);
        regRows = regDao.retrieveRows(officeId, tsId, textMask,
                startTime, endTime, versionDate, kiloByteLimit, urlBuilder);

        VersionType versionType = getVersionType(tsId, officeId, versionDate != null);

        return new TextTimeSeries.Builder()
                .withOfficeId(officeId)
                .withName(tsId)
                .withRegularTextValues(regRows)
                .withVersionDate(versionDate)
                .withDateVersionType(versionType)
                .build();
    }

    @NotNull
    private VersionType getVersionType(String names, String office, boolean versionDateProvided) {
        VersionType dateVersionType;

        if (versionDateProvided) {
            dateVersionType = VersionType.SINGLE_VERSION;
        } else {
            boolean isVersioned = connectionResult(dsl, connection -> {
                Configuration configuration = getDslContext(connection, office).configuration();
                return OracleTypeMap.parseBool(CWMS_TS_PACKAGE.call_IS_TSID_VERSIONED(configuration,
                        names, office));
            });

            if (isVersioned) {
                dateVersionType = VersionType.MAX_AGGREGATE;
            } else {
                dateVersionType = VersionType.UNVERSIONED;
            }
        }

        return dateVersionType;
    }


    public void create(TextTimeSeries tts, boolean replaceAll) {
        Instant versionDate = tts.getVersionDate();
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.storeRows(tts.getOfficeId(), tts.getName(), replaceAll, regRows, versionDate);
        }
    }
    
    public void store(TextTimeSeries tts, boolean replaceAll) {

        Instant versionDate = tts.getVersionDate();
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.storeRows(tts.getOfficeId(), tts.getName(), replaceAll, regRows, versionDate);
        }

    }
    

    public void delete(String officeId, String textTimeSeriesId, String textMask,
                       @NotNull Instant start, @NotNull Instant end,
                        @Nullable Instant versionDate) {

        RegularTimeSeriesTextDao regDao = getRegularDao();
        regDao.delete(officeId, textTimeSeriesId, textMask,
                start, end, versionDate);
    }

    @NotNull
    private RegularTimeSeriesTextDao getRegularDao(){
        return new RegularTimeSeriesTextDao(dsl);
    }
}
