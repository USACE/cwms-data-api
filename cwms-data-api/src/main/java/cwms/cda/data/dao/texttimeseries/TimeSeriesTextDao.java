package cwms.cda.data.dao.texttimeseries;

import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

// based on https://bitbucket.hecdev.net/projects/CWMS/repos/hec-cwms-data-access/browse/hec-db-jdbc/src/main/java/wcds/dbi/oracle/cwms/CwmsTimeSeriesTextJdbcDao.java
public final class TimeSeriesTextDao extends JooqDao<TextTimeSeries> {

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
        List<RegularTextTimeSeriesRow> regRows;

        RegularTimeSeriesTextDao regDao = getRegularDao();
        Instant instant = null;
        if (versionDate != null) {
            instant = versionDate.toInstant();
        }
        regRows = regDao.retrieveRows(officeId, tsId, textMask,
                startTime.toInstant(), endTime.toInstant(), instant,
                maxVersion, minAttribute, maxAttribute);

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


    public void create(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {
        ZonedDateTime versionDate = tts.getVersionDate();
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();
            regDao.storeRows(tts.getOfficeId(), tts.getName(), maxVersion, replaceAll, regRows,
                    versionDate == null ? null : versionDate.toInstant());
        }
    }
    
    public void store(TextTimeSeries tts, boolean maxVersion, boolean replaceAll) {

        ZonedDateTime versionDate = tts.getVersionDate();
        Collection<RegularTextTimeSeriesRow> regRows = tts.getRegularTextValues();
        if (regRows != null) {
            RegularTimeSeriesTextDao regDao = getRegularDao();

            for (RegularTextTimeSeriesRow regRow : regRows) {
                regDao.storeRow(tts.getOfficeId(), tts.getName(), regRow, maxVersion, replaceAll,
                        versionDate == null ? null : versionDate.toInstant());
            }

        }

    }
    

    public void delete(String officeId, String textTimeSeriesId, String textMask,
                       @NotNull ZonedDateTime start, @NotNull ZonedDateTime end,
                        @Nullable ZonedDateTime versionDate, boolean maxVersion,
                        Long minAttribute, Long maxAttribute) {
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
