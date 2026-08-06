package cwms.cda.data.dao;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.v2.ForecastLocation;
import cwms.cda.data.dto.v2.ForecastSpecV2;

import org.jooq.Condition;
import org.jooq.Record2;
import org.jooq.Record5;
import org.jooq.Record6;
import org.jooq.SelectConditionStep;
import org.jooq.SelectOnConditionStep;
import org.jooq.Table;
import usace.cwms.db.jooq.codegen_latest.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen_latest.packages.cwms_fcst.RETRIEVE_FCST_SPEC_WITH_LOCATIONS;
import usace.cwms.db.jooq.codegen_latest.tables.AV_FCST_LOCATION;
import usace.cwms.db.jooq.codegen_latest.tables.AV_FCST_SPEC;
import usace.cwms.db.jooq.codegen_latest.tables.AV_FCST_TIME_SERIES;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen_latest.udt.records.FCST_LOCATION_T;
import usace.cwms.db.jooq.codegen_latest.udt.records.FCST_LOCATION_TAB_T;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public final class ForecastSpecDaoV2 extends AbstractForecastSpecDao<ForecastSpecV2> {

    public ForecastSpecDaoV2(DSLContext dsl) {
        super(dsl);
    }

    @Override
    protected ViewWrapper getViewWrapper() {
        AV_FCST_SPEC view = AV_FCST_SPEC.AV_FCST_SPEC;
        return new ViewWrapper(view.OFFICE_ID, view.FCST_SPEC_ID, view.FCST_DESIGNATOR, view.ENTITY_ID,
                view.FCST_SPEC_CODE);
    }

    @Override
    public void create(ForecastSpecV2 forecastSpec) {

        connection(dsl, conn -> {
            setOffice(conn, forecastSpec.getSpecId().getOfficeId());

            String timeSeriesIds = null;
            if (forecastSpec.getTimeSeriesIds() != null) {
                timeSeriesIds = String.join("\n", forecastSpec.getTimeSeriesIds());
            }

            FCST_LOCATION_TAB_T locations = new FCST_LOCATION_TAB_T();
            if (forecastSpec.getLocationIds() != null) {
                for (ForecastLocation fcstLocation : forecastSpec.getLocationIds()) {
                    FCST_LOCATION_T locationRecord = new FCST_LOCATION_T();
                    locationRecord.setLOCATION_ID(fcstLocation.getLocationId());
                    locationRecord.setSORT_ORDER(BigDecimal.valueOf(fcstLocation.getSortOrder()));
                    locations.add(locationRecord);
                }
            }
            CWMS_FCST_PACKAGE.call_STORE_FCST_SPEC_WITH_LOCATIONS(DSL.using(conn).configuration(),
                    forecastSpec.getSpecId().getName(), forecastSpec.getDesignator(), forecastSpec.getSourceEntityId(), forecastSpec.getDescription(),
                    locations, timeSeriesIds, "F", "F", forecastSpec.getSpecId().getOfficeId());
        });
    }

    @Override
    public List<ForecastSpecV2> getForecastSpecs(String office, String specIdRegex,
            String designator, String sourceEntityRegex, String entityLike) {

        ViewWrapper wrapper = getViewWrapper();
        Condition condition = buildSpecListCondition(wrapper, office, specIdRegex, designator, sourceEntityRegex,
                entityLike);

        SelectConditionStep<Record6<String, String, String, String, String, String>> query =
            forecastSpecQuery(dsl).where(condition);
        List<ForecastSpecV2> specs = query.fetch().map(ForecastSpecDaoV2::map);

        Map<String, List<ForecastLocation>> locationsByKey = fetchLocationsFor(condition);

        List<ForecastSpecV2> results = new ArrayList<>();
        for (ForecastSpecV2 s : specs) {
            results.add(withLocations(s, locationsByKey));
        }
        return results;
    }

    @Override
    public ForecastSpecV2 getForecastSpec(String office, String specId, String designator) {
        return connectionResult(dsl, conn -> {
            RETRIEVE_FCST_SPEC_WITH_LOCATIONS retrieved = CWMS_FCST_PACKAGE.call_RETRIEVE_FCST_SPEC_WITH_LOCATIONS(DSL.using(conn).configuration(), specId, designator, office);
            List<String> tsIds = new ArrayList<>();
            if(retrieved.getP_TIMESERIES_IDS() != null && !retrieved.getP_TIMESERIES_IDS().isEmpty()) {
                tsIds = List.of(retrieved.getP_TIMESERIES_IDS().split("\\r?\\n"));
            }
            return new ForecastSpecV2.Builder()
                    .withSpecId(new CwmsId.Builder()
                            .withOfficeId(office)
                            .withName(retrieved.getName())
                            .build())
                    .withDescription(retrieved.getP_DESCRIPTION())
                    .withDesignator(designator)
                    .withTimeSeriesIds(tsIds)
                    .withSourceEntityId(retrieved.getP_ENTITY_ID())
                    .withLocationIds(retrieved.getP_LOCATION_IDS().stream()
                            .map(loc -> new ForecastLocation.Builder()
                                    .withLocationId(loc.getLOCATION_ID())
                                    .withSortOrder(loc.getSORT_ORDER().intValue())
                                    .build())
                            .collect(toList()))
                    .build();
        });
    }

    /**
     * Spec-level projection: identical to V1's, minus the single location-id column.
     * One row per spec; locations are fetched and attached separately (see class Javadoc).
     */
    private static SelectOnConditionStep<Record6<String, String, String, String, String, String>>
            forecastSpecQuery(DSLContext dsl) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        AV_FCST_TIME_SERIES timeSeries = AV_FCST_TIME_SERIES.AV_FCST_TIME_SERIES;
        //Group all the timeseries ids into a "\n" delimited list
        Table<Record2<String, String>> tsidTable = dsl.select(timeSeries.FCST_SPEC_CODE,
                        DSL.listAgg(timeSeries.CWMS_TS_ID, "\n")
                                .withinGroupOrderBy(timeSeries.CWMS_TS_ID)
                                .as("time_series_list"))
                .from(timeSeries)
                .groupBy(timeSeries.FCST_SPEC_CODE)
                .asTable("tsids");
        return dsl.select(spec.FCST_SPEC_ID, spec.DESCRIPTION, spec.FCST_DESIGNATOR,
                        spec.OFFICE_ID, tsidTable.field("time_series_list", String.class), spec.ENTITY_ID)
                .from(spec)
                .leftJoin(tsidTable)
                .on(spec.FCST_SPEC_CODE.eq(tsidTable.field("FCST_SPEC_CODE", String.class)));
    }

    private static ForecastSpecV2 map(Record6<String, String, String, String, String, String> r) {
        List<String> timeSeriesIdentifiers = new ArrayList<>();
        if (r.value5() != null) {
            timeSeriesIdentifiers = Arrays.stream(r.value5().split("\n")).collect(toList());
        }
        return new ForecastSpecV2.Builder()
                .withSpecId(new CwmsId.Builder()
                        .withOfficeId(r.value4())
                        .withName(r.value1())
                        .build())
                .withDescription(r.value2())
                .withDesignator(r.value3())
                .withTimeSeriesIds(timeSeriesIdentifiers)
                .withSourceEntityId(r.value6())
                .build();
    }

    /**
     * Fetches every location row for specs matching {@code specCondition}, joined against
     * {@code AV_FCST_SPEC} so each row also carries its spec's business key, and grouped by
     * that key. Ordered by sort order within each spec so the grouped lists come out in
     * the right order without any further sorting in Java.
     */
    private Map<String, List<ForecastLocation>> fetchLocationsFor(Condition specCondition) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        AV_FCST_LOCATION loc = AV_FCST_LOCATION.AV_FCST_LOCATION;

        // Only SORT_ORDER is selected from AV_FCST_LOCATION, not its derived IS_PRIMARY
        // column: FCST_LOCATION_T (used by create/getForecastSpec) has no is-primary
        // attribute either, so sort order -1 is the single source of truth for "primary"
        // across this whole feature. ForecastLocation.Builder derives isPrimary from
        // sortOrder automatically, keeping this in sync with getForecastSpec's mapping.
        List<Record5<String, String, String, String, BigDecimal>> rows = dsl.select(
                        spec.OFFICE_ID, spec.FCST_SPEC_ID, spec.FCST_DESIGNATOR,
                        loc.LOCATION_ID, loc.SORT_ORDER)
                .from(spec)
                .join(loc)
                .on(spec.FCST_SPEC_CODE.eq(loc.FCST_SPEC_CODE))
                .where(specCondition)
                .orderBy(spec.FCST_SPEC_ID, loc.SORT_ORDER)
                .fetch();

        Map<String, List<ForecastLocation>> locationsByKey = new HashMap<>();
        for (Record5<String, String, String, String, BigDecimal> row : rows) {
            String key = specKey(row.value1(), row.value2(), row.value3());
            ForecastLocation location = new ForecastLocation.Builder()
                    .withLocationId(row.value4())
                    .withSortOrder(row.value5().intValue())
                    .build();
            locationsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(location);
        }
        return locationsByKey;
    }

    private static ForecastSpecV2 withLocations(ForecastSpecV2 spec, Map<String, List<ForecastLocation>> locationsByKey) {
        String key = specKey(spec.getSpecId().getOfficeId(), spec.getSpecId().getName(), spec.getDesignator());
        List<ForecastLocation> locations = locationsByKey.get(key);
        return new ForecastSpecV2.Builder()
                .from(spec)
                .withLocationIds(locations)
                .build();
    }

    /** Composite business key for a spec: office + spec id + designator (nullable). */
    private static String specKey(String office, String specId, String designator) {
        return office + ' ' + specId + ' ' + (designator == null ? "" : designator);
    }

    @Override
    public void update(ForecastSpecV2 forecastSpec) {
        //Will throw NotFoundException is spec does not exist
        getForecastSpec(forecastSpec.getSpecId().getOfficeId(), forecastSpec.getSpecId().getName(), forecastSpec.getDesignator());
        create(forecastSpec);
    }
}
