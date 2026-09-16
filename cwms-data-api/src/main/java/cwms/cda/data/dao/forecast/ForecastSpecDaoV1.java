package cwms.cda.data.dao.forecast;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.forecast.ForecastSpec;

import org.jetbrains.annotations.NotNull;
import org.jooq.SelectConditionStep;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_LOCATION;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_TIME_SERIES;

import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record7;
import org.jooq.SelectOnConditionStep;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * V1 forecast spec DAO: a forecast spec has a single {@code location-id}. See
 * {@link ForecastSpecDaoV2} for the V2 shape ({@code List<ForecastLocation>}), and
 * {@link ForecastSpecDao} for the logic (delete, and the office/spec-id/
 * designator/source-entity filters) shared between the two.
 */
public final class ForecastSpecDaoV1 extends ForecastSpecDao<ForecastSpec> {

    public ForecastSpecDaoV1(DSLContext dsl) {
        super(dsl);
    }


    @Override
    protected ViewWrapper getViewWrapper() {
        AV_FCST_SPEC view = AV_FCST_SPEC.AV_FCST_SPEC;
        return new ViewWrapper(view.OFFICE_ID, view.FCST_SPEC_ID, view.FCST_DESIGNATOR, view.ENTITY_ID,
                view.FCST_SPEC_CODE);
    }

    @Override
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

    @Override
    public List<ForecastSpec> getForecastSpecs(String office, String specIdRegex,
            String designator, String sourceEntityRegex, String entityLike) {
        ViewWrapper wrapper = getViewWrapper();
        SelectConditionStep<Record7<String, String, String, String, String, String, String>> query =
            forecastSpecQuery(dsl)
                .where(buildSpecListCondition(wrapper, office, specIdRegex, designator, sourceEntityRegex, entityLike));
        return query.fetch()
               .map(ForecastSpecDaoV1::map);
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

    @Override
    public ForecastSpec getForecastSpec(@NotNull String office, String name, String designator) {
        ViewWrapper wrapper = getViewWrapper();
        SelectConditionStep<Record7<String, String, String, String, String, String, String>> query =
            forecastSpecQuery(dsl)
                .where(buildSpecKeyCondition(wrapper, office, name, designator));
        Record7<String, String, String, String, String, String, String> fetch = query.fetchOne();
        if (fetch == null) {
            throw new NotFoundException(
                format("Could not find forecast instance for office id: %s, spec id: %s, designator: %s",
                    office, name, designator));
        }
        return map(fetch);
    }

    @Override
    public void update(ForecastSpec forecastSpec) {
        //Will throw NotFoundException is spec does not exist
        getForecastSpec(forecastSpec.getOfficeId(), forecastSpec.getSpecId(), forecastSpec.getDesignator());
        create(forecastSpec);
    }
}
