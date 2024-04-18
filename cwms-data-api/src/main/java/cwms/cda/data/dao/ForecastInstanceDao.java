package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.forecast.ForecastInstance;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.json.JsonV2;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.Record;
import org.jooq.Record16;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.SelectOnConditionStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_INST;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_LOCATION;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_SPEC;
import usace.cwms.db.jooq.codegen.tables.AV_FCST_TIME_SERIES;
import usace.cwms.db.jooq.codegen.tables.AV_TS_EXTENTS_UTC;
import usace.cwms.db.jooq.codegen.udt.records.BLOB_FILE_T;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class ForecastInstanceDao extends JooqDao<ForecastInstance> {

    public ForecastInstanceDao(DSLContext dsl) {
        super(dsl);
    }

    public void create(ForecastInstance forecastInst) {
        String officeId = forecastInst.getSpec().getOfficeId();
        Timestamp forecastDate = Timestamp.from(forecastInst.getDateTime());
        Timestamp issueDate = Timestamp.from(forecastInst.getIssueDateTime());
        String forecastInfo = mapToJson(forecastInst.getMetadata());
        byte[] fileData = forecastInst.getFileData();
        BLOB_FILE_T blob = new BLOB_FILE_T(forecastInst.getFilename(), null, OffsetDateTime.now(), 0L, fileData);
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_FCST_PACKAGE.call_STORE_FCST(DSL.using(conn).configuration(), forecastInst.getSpec().getSpecId(),
                    forecastInst.getSpec().getDesignator(), forecastDate, issueDate,
                    "UTC", forecastInst.getMaxAge(), forecastInst.getNotes(), forecastInfo,
                    blob, "F", "T", officeId);
        });
    }

    private static String mapToJson(Map<String, String> metadata) {
        try {
            return JsonV2.buildObjectMapper().writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error serializing forecast info to JSON", e);
        }
    }

    private static Map<String, String> mapFromJson(String forecastInfo) {
        try {
            return JsonV2.buildObjectMapper().readValue(forecastInfo, new TypeReference<Map<String, String>>() {
            });
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error serializing forecast info to JSON", e);
        }
    }

    public List<ForecastInstance> getForecastInstances(String office, String name, String designator) {
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        return instanceQuery().where(JooqDao.filterExact(spec.OFFICE_ID, office))
                .and(JooqDao.filterExact(spec.FCST_SPEC_ID, name))
                .and(JooqDao.filterExact(spec.FCST_DESIGNATOR, designator))
                .fetch().map(ForecastInstanceDao::map);
    }

    private static ForecastInstance map(Record16<String, String, String, String, String, String, Timestamp, Timestamp,
            Integer, String, String, String, String, Timestamp, Timestamp, JSON> r) {
        List<String> timeSeriesIdentifiers = new ArrayList<>();
        if (r.value5() != null) {
            timeSeriesIdentifiers = Arrays.stream(r.value5().split("\n")).collect(toList());
        }
        Instant forecastDate = r.value7().toInstant();
        Instant issueDate = r.value8().toInstant();
        Map<String, String> forecastInfo = new HashMap<>();
        JSON json = r.value16();
        if (json != null) {
            forecastInfo = mapFromJson(json.data());
        }
        return new ForecastInstance.Builder()
                .withSpec(new ForecastSpec.Builder()
                        .withSpecId(r.value1())
                        .withDescription(r.value2())
                        .withDesignator(r.value3())
                        .withOfficeId(r.value4())
                        .withTimeSeriesIds(timeSeriesIdentifiers)
                        .withSourceEntityId(r.value6())
                        .withLocationId(r.value13())
                        .build())
                .withDateTime(forecastDate)
                .withIssueDateTime(issueDate)
                .withMaxAge(r.value9())
                .withFilename(r.value10())
                .withFileMediaType(r.value11())
                .withNotes(r.value12())
                //Currently the views don't provide us with this information
                .withFirstDateTime(r.value14().toInstant())
                .withLastDateTime(r.value15().toInstant())
                .withMetadata(forecastInfo)
                .build();
    }

    private @NotNull SelectOnConditionStep<Record16<String, String, String, String, String, String,
            Timestamp, Timestamp, Integer, String, String, String,
            String, Timestamp, Timestamp, JSON>> instanceQuery() {
        AV_FCST_INST inst = AV_FCST_INST.AV_FCST_INST;
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        AV_FCST_TIME_SERIES timeSeries = AV_FCST_TIME_SERIES.AV_FCST_TIME_SERIES;
        AV_TS_EXTENTS_UTC extentsUtc = AV_TS_EXTENTS_UTC.AV_TS_EXTENTS_UTC;
        AV_FCST_LOCATION loc = AV_FCST_LOCATION.AV_FCST_LOCATION;

        //Group all the timeseries ids into a "\n" delimited list so we can take them apart later
        Table<Record2<String, String>> tsidTable = dsl
                .select(spec.FCST_SPEC_CODE,
                        DSL.listAgg(timeSeries.CWMS_TS_ID, "\n").withinGroupOrderBy(timeSeries.CWMS_TS_ID)
                                .as("time_series_list"))
                .from(timeSeries)
                .groupBy(timeSeries.FCST_SPEC_CODE)
                .asTable("tsids");
        Table<Record3<String, String, String>> extentsTable = dsl
                .select(spec.FCST_SPEC_CODE,
                        DSL.min(timeSeries.CWMS_TS_ID).as("first_date_time"),
                        DSL.max(timeSeries.CWMS_TS_ID).as("last_date_time"))
                .from(timeSeries)
                .leftJoin(extentsUtc)
                .on(timeSeries.TS_CODE.coerce(BigInteger.class).eq(extentsUtc.TS_CODE))
                .groupBy(timeSeries.FCST_SPEC_CODE)
                .asTable("extents");
        Table<Record4<String, Timestamp, Timestamp, JSON>> infoTable = dsl
                .select(field("AT_FCST_INST.FCST_SPEC_CODE", String.class),
                        field("AT_FCST_INST.FCST_DATE_TIME", Timestamp.class),
                        field("AT_FCST_INST.ISSUE_DATE_TIME", Timestamp.class),
                        DSL.jsonObjectAgg(field("AT_FCST_INFO.KEY", String.class),
                                        field("AT_FCST_INFO.VALUE", String.class))
                                .as("fcst_info"))
                .from(table("AT_FCST_INST"))
                .leftJoin(table("AT_FCST_INFO"))
                .on(field("AT_FCST_INST.FCST_INST_CODE").eq("AT_FCST_INFO.FCST_INST_CODE"))
                .groupBy(field("AT_FCST_INST.FCST_INST_CODE"))
                .asTable("info");
        Table<Record> specTable = dsl.select(spec.FCST_SPEC_ID, spec.DESCRIPTION, spec.FCST_DESIGNATOR,
                        spec.OFFICE_ID, tsidTable.field("time_series_list", String.class), spec.ENTITY_ID)
                .select(tsidTable.field("time_series_list"))
                .from(spec)
                .leftJoin(tsidTable)
                .on(spec.FCST_SPEC_CODE.eq(tsidTable.field("FCST_SPEC_CODE", String.class)))
                .asTable("spec");
        return dsl.select(spec.FCST_SPEC_ID, spec.DESCRIPTION,
                        spec.FCST_DESIGNATOR, spec.OFFICE_ID, tsidTable.field("time_series_list", String.class),
                        spec.ENTITY_ID, inst.FCST_DATE_TIME_UTC, inst.ISSUE_DATE_TIME_UTC, inst.VALID_HOURS,
                        inst.FILE_NAME, inst.FILE_MEDIA_TYPE, inst.NOTES, loc.LOCATION_ID,
                        extentsTable.field("first_date_time", Timestamp.class),
                        extentsTable.field("last_date_time", Timestamp.class),
                        infoTable.field("fcst_info", JSON.class))
                .from(inst)
                .leftJoin(specTable)
                .on(spec.FCST_SPEC_ID.eq(specTable.field("FCST_SPEC_ID", String.class)))
                .leftJoin(loc)
                .on(spec.FCST_SPEC_CODE.eq(loc.FCST_SPEC_CODE))
                .leftJoin(extentsTable)
                .on(spec.FCST_SPEC_CODE.eq(extentsTable.field("FCST_SPEC_CODE", String.class)))
                .leftJoin(infoTable)
                .on(spec.FCST_SPEC_CODE.eq(field("info.FCST_SPEC_CODE", String.class)))
                .and(inst.FCST_DATE_TIME_UTC.eq(field("info.FCST_DATE_TIME", Timestamp.class)))
                .and(inst.ISSUE_DATE_TIME_UTC.eq(field("info.ISSUE_DATE_TIME", Timestamp.class)));
    }

    public ForecastInstance getForecastInstance(String office, String name, String designator,
            Instant forecastDate, Instant issueDate) {

        AV_FCST_INST inst = AV_FCST_INST.AV_FCST_INST;
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        Record16<String, String, String, String, String, String,
                Timestamp, Timestamp, Integer, String, String, String, String,
                Timestamp, Timestamp, JSON> fetch = instanceQuery()
                .where(spec.OFFICE_ID.eq(office))
                .and(spec.FCST_SPEC_ID.eq(name))
                .and(spec.FCST_DESIGNATOR.eq(designator))
                .and(inst.FCST_DATE_TIME_UTC.eq(Timestamp.from(forecastDate)))
                .and(inst.ISSUE_DATE_TIME_UTC.eq(Timestamp.from(issueDate)))
                .fetchOne();
        if (fetch == null) {
            String message = format("Could not find forecast instance for " +
                            "office id: %s, spec id: %s, designator: %s, forecast date: %s, issue date: %s",
                    office, name, designator, forecastDate, issueDate);
            throw new NotFoundException(message);
        }
        return map(fetch);
    }

    public void update(ForecastInstance forecastInst) {
        String officeId = forecastInst.getSpec().getOfficeId();
        String specId = forecastInst.getSpec().getSpecId();
        String designator = forecastInst.getSpec().getDesignator();
        Instant forecastDate = forecastInst.getDateTime();
        Instant issueDate = forecastInst.getIssueDateTime();
        AV_FCST_INST inst = AV_FCST_INST.AV_FCST_INST;
        AV_FCST_SPEC spec = AV_FCST_SPEC.AV_FCST_SPEC;
        Record16<String, String, String, String, String, String,
                Timestamp, Timestamp, Integer, String, String, String, String,
                Timestamp, Timestamp, JSON> fetch = instanceQuery()
                .where(spec.OFFICE_ID.eq(officeId))
                .and(spec.FCST_SPEC_ID.eq(specId))
                .and(spec.FCST_DESIGNATOR.eq(designator))
                .and(inst.FCST_DATE_TIME_UTC.eq(Timestamp.from(forecastDate)))
                .and(inst.ISSUE_DATE_TIME_UTC.eq(Timestamp.from(issueDate)))
                .fetchOne();
        if (fetch == null) {
            throw new NotFoundException("Forecast instance not found: " + forecastInst);
        }
        create(forecastInst);
    }

    public void delete(String office, String name, String designator,
            Instant forecastDate, Instant issueDate) {
        connection(dsl, conn -> {
            setOffice(conn, office);
            CWMS_FCST_PACKAGE.call_DELETE_FCST(DSL.using(conn).configuration(), name, designator, Timestamp.from(forecastDate),
                    Timestamp.from(issueDate), "UTC", office);
        });
    }

}
