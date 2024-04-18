package cwms.cda.data.dao;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.forecast.ForecastSpec;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record7;
import org.jooq.SelectOnConditionStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_LOCATION;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_TIME_SERIES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ForecastSpecDao extends JooqDao<ForecastSpec> {

    public ForecastSpecDao(DSLContext dsl) {
        super(dsl);
    }


    public void create(ForecastSpec forecastSpec) {
        connection(dsl, conn -> {
            setOffice(conn, forecastSpec.getOfficeId());

            String timeSeriesIds = null;
            if (forecastSpec.getTimeSeriesIds() != null) {
                timeSeriesIds = String.join("\n", forecastSpec.getTimeSeriesIds());
            }
            CWMS_FCST_PACKAGE.call_STORE_FCST_SPEC(DSL.using(conn).configuration(), forecastSpec.getSpecId(),
                    forecastSpec.getDesignator(), forecastSpec.getSourceEntityId(),
                    forecastSpec.getDescription(), forecastSpec.getLocationId(),
                    timeSeriesIds, "F", "F", forecastSpec.getOfficeId());
        });
    }

    public void delete(String office, String specId, String designator, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, office);
            CWMS_FCST_PACKAGE.call_DELETE_FCST_SPEC(DSL.using(conn).configuration(), specId, designator,
                    deleteRule.getRule(), office);
        });
    }

    public List<ForecastSpec> getForecastSpecs(String office, String specIdRegex,
            String designator, String sourceEntity) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        return forecastSpecQuery(dsl)
                .where(JooqDao.caseInsensitiveLikeRegex(spec.OFFICE_ID, office))
                .and(JooqDao.caseInsensitiveLikeRegex(spec.FCST_SPEC_ID, specIdRegex))
                .and(JooqDao.caseInsensitiveLikeRegex(spec.FCST_DESIGNATOR, designator))
                .and(JooqDao.caseInsensitiveLikeRegex(spec.ENTITY_ID, sourceEntity))
                .fetch()
                .map(ForecastSpecDao::map);
    }

    private static SelectOnConditionStep<Record7<String, String, String, String,
            String, String, String>> forecastSpecQuery(DSLContext dsl) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        AV_FCST_TIME_SERIES timeSeries = AV_FCST_TIME_SERIES.AV_FCST_TIME_SERIES;
        AV_FCST_LOCATION loc = AV_FCST_LOCATION.AV_FCST_LOCATION;
        //Group all the timeseries ids into a "\n" delimited list
        Table<Record2<String, String>> tsidTable = dsl.select(timeSeries.FCST_SPEC_CODE,
                        DSL.listAgg(timeSeries.CWMS_TS_ID, "\n")
                                .withinGroupOrderBy(timeSeries.CWMS_TS_ID)
                                .as("time_series_list"))
                .from(timeSeries)
                .groupBy(timeSeries.FCST_SPEC_CODE)
                .asTable("tsids");
        return dsl.select(spec.FCST_SPEC_ID, spec.DESCRIPTION, spec.FCST_DESIGNATOR,
                        spec.OFFICE_ID, tsidTable.field("time_series_list", String.class), spec.ENTITY_ID, loc.LOCATION_ID)
                .from(spec)
                .leftJoin(tsidTable)
                .on(spec.FCST_SPEC_CODE.eq(tsidTable.field("FCST_SPEC_CODE", String.class)))
                .leftJoin(loc)
                .on(spec.FCST_SPEC_CODE.eq(loc.FCST_SPEC_CODE));
    }

    private static ForecastSpec map(Record7<String, String, String, String, String, String, String> r) {
        List<String> timeSeriesIdentifiers = new ArrayList<>();
        if (r.value5() != null) {
            timeSeriesIdentifiers = Arrays.stream(r.value5().split("\n")).collect(toList());
        }
        return new ForecastSpec.Builder()
                .withSpecId(r.value1())
                .withDescription(r.value2())
                .withDesignator(r.value3())
                .withOfficeId(r.value4())
                .withTimeSeriesIds(timeSeriesIdentifiers)
                .withSourceEntityId(r.value6())
                .withLocationId(r.value7())
                .build();
    }

    public ForecastSpec getForecastSpec(String office, String name, String designator) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;

        Record7<String, String, String, String, String, String, String> fetch = forecastSpecQuery(dsl)
                .where(spec.OFFICE_ID.eq(office))
                .and(spec.FCST_SPEC_ID.eq(name))
                .and(spec.FCST_DESIGNATOR.eq(designator))
                .fetchOne();
        if (fetch == null) {
            throw new NotFoundException(
                    format("Could not find forecast instance for office id: %s, spec id: %s, designator: %s",
                            office, name, designator));
        }
        return map(fetch);
    }

    public void update(ForecastSpec forecastSpec) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        Record7<String, String, String, String, String, String, String> fetch = forecastSpecQuery(dsl)
                .where(spec.OFFICE_ID.eq(forecastSpec.getOfficeId()))
                .and(spec.FCST_SPEC_ID.eq(forecastSpec.getSpecId()))
                .and(spec.FCST_DESIGNATOR.eq(forecastSpec.getDesignator()))
                .fetchOne();
        if (fetch == null) {
            throw new NotFoundException("Forecast spec not found: " + forecastSpec);
        }
        create(forecastSpec);
    }
}
