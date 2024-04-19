package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.forecast.ForecastInstance;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.ReplaceUtils;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultBinding;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.BLOB_FILE_T;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ForecastInstanceDao extends JooqDao<ForecastInstance> {

    private static final Calendar UTC_CALENDAR = Calendar.getInstance(OracleTypeMap.GMT_TIME_ZONE);
    private static final String INSTANCE_QUERY = "select spec_id," +
            "       spec_description," +
            "       spec_designator," +
            "       spec_office_id," +
            "       time_series_list," +
            "       spec_source_entity," +
            "       inst.FCST_DATE_TIME," +
            "       inst.ISSUE_DATE_TIME," +
            "       inst.MAX_AGE," +
            "       inst.BLOB_FILE," +
            "       inst.NOTES," +
            "       loc.LOCATION_ID," +
            "       first_date_time," +
            "       last_date_time," +
            "       fcst_info_json" +
            " from CWMS_20.AT_FCST_INST inst" +
            "         left outer join (select CWMS_20.AT_FCST_SPEC.FCST_SPEC_ID AS spec_id," +
            "                                 CWMS_20.AT_FCST_SPEC.DESCRIPTION AS spec_description," +
            "                                 CWMS_20.AT_FCST_SPEC.FCST_DESIGNATOR AS spec_designator," +
            "                                 CWMS_20.CWMS_UTIL.GET_DB_OFFICE_ID_FROM_CODE(CWMS_20.AT_FCST_SPEC.OFFICE_CODE) AS spec_office_id," +
            "                                 CWMS_20.AT_FCST_SPEC.FCST_SPEC_CODE," +
            "                                 tsids.time_series_list AS time_series_list," +
            "                                 (SELECT ENTITY_ID" +
            "                                  FROM CWMS_20.AT_ENTITY" +
            "                                  WHERE ENTITY_CODE = CWMS_20.AT_FCST_SPEC.SOURCE_ENTITY)                       AS spec_source_entity" +
            "                          from CWMS_20.AT_FCST_SPEC" +
            "                                   left outer join (select CWMS_20.AV_FCST_TIME_SERIES.FCST_SPEC_CODE," +
            "                                                           listagg(CWMS_20.AV_FCST_TIME_SERIES.CWMS_TS_ID, '\\n')" +
            "                                                                   within group (order by CWMS_20.AV_FCST_TIME_SERIES.CWMS_TS_ID) time_series_list" +
            "                                                    from CWMS_20.AV_FCST_TIME_SERIES" +
            "                                                    group by CWMS_20.AV_FCST_TIME_SERIES.FCST_SPEC_CODE) tsids" +
            "                                                   on CWMS_20.AT_FCST_SPEC.FCST_SPEC_CODE = tsids.FCST_SPEC_CODE) spec" +
            "                         on inst.FCST_SPEC_CODE = spec.FCST_SPEC_CODE" +
            "         left outer join CWMS_20.AV_FCST_LOCATION loc" +
            "                         on inst.FCST_SPEC_CODE = loc.FCST_SPEC_CODE" +
            "         left outer join (select CWMS_20.AT_FCST_TIME_SERIES.FCST_SPEC_CODE  extents_spec_code," +
            "                                 min(CWMS_20.AT_TS_EXTENTS.EARLIEST_TIME) first_date_time," +
            "                                 max(CWMS_20.AT_TS_EXTENTS.LATEST_TIME) last_date_time" +
            "                          from CWMS_20.AT_FCST_TIME_SERIES" +
            "                                   left outer join CWMS_20.AT_TS_EXTENTS" +
            "                                                   on CWMS_20.AT_FCST_TIME_SERIES.TS_CODE =" +
            "                                                      CWMS_20.AT_TS_EXTENTS.TS_CODE" +
            "                          group by CWMS_20.AT_FCST_TIME_SERIES.FCST_SPEC_CODE) extents" +
            "                         on inst.FCST_SPEC_CODE = extents_spec_code" +
            "         left outer join (select info.FCST_INST_CODE AS info_inst_code," +
            "                                 json_objectagg(key info.KEY value info.VALUE) fcst_info_json" +
            "                          from CWMS_20.AT_FCST_INFO info" +
            "                          group by info.FCST_INST_CODE) fcst_info" +
            "                         on (inst.FCST_INST_CODE = info_inst_code)";

    private static final String FILE_QUERY = "select inst.BLOB_FILE" +
            " from CWMS_20.AT_FCST_INST inst" +
            "         left outer join CWMS_20.AT_FCST_SPEC spec on inst.FCST_SPEC_CODE = spec.FCST_SPEC_CODE";

    private static final String GET_ALL_CONDITIONS = " WHERE (? IS NULL OR spec_office_id = ?)" +
            " AND (? IS NULL OR spec_id = ?)" +
            " AND (? IS NULL OR spec_designator = ?)";
    private static final String GET_ONE_CONDITIONS = " WHERE (spec_office_id = ?)" +
            " AND (spec_id = ?)" +
            " AND (spec_designator = ?)" +
            " AND (inst.FCST_DATE_TIME = cwms_20.cwms_util.to_timestamp(?))" +
            " AND (inst.ISSUE_DATE_TIME = cwms_20.cwms_util.to_timestamp(?))";
    private static final String FILE_CONDITIONS = " WHERE (spec.OFFICE_CODE = CWMS_UTIL.GET_DB_OFFICE_CODE(?))" +
            " AND (spec.FCST_SPEC_ID = ?)" +
            " AND (spec.FCST_DESIGNATOR = ?)" +
            " AND (inst.FCST_DATE_TIME = cwms_20.cwms_util.to_timestamp(?))" +
            " AND (inst.ISSUE_DATE_TIME = cwms_20.cwms_util.to_timestamp(?))";

    public ForecastInstanceDao(DSLContext dsl) {
        super(dsl);
    }

    public void create(ForecastInstance forecastInst) {
        String officeId = forecastInst.getSpec().getOfficeId();
        Timestamp forecastDate = Timestamp.from(forecastInst.getDateTime());
        Timestamp issueDate = Timestamp.from(forecastInst.getIssueDateTime());
        String forecastInfo = mapToJson(forecastInst.getMetadata());
        byte[] fileData = forecastInst.getFileData();
        BLOB_FILE_T blob = new BLOB_FILE_T(forecastInst.getFilename(), forecastInst.getFileMediaType(), OffsetDateTime.now(), 0L, fileData);
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            DefaultBinding.THREAD_LOCAL.set(UTC_CALENDAR);
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

    public List<ForecastInstance> getForecastInstances(int byteLimit, ReplaceUtils.OperatorBuilder urlBuilder,
            String office, String name, String designator) {

        String query = INSTANCE_QUERY + GET_ALL_CONDITIONS;
        return connectionResult(dsl, (Connection c) -> {
            try (PreparedStatement preparedStatement = c.prepareStatement(query)) {
                preparedStatement.setString(1, office);
                preparedStatement.setString(2, name);
                preparedStatement.setString(3, designator);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    List<ForecastInstance> instances = new ArrayList<>();
                    while (resultSet.next()) {
                        instances.add(map(byteLimit, urlBuilder, resultSet));
                    }
                    return instances;
                }
            }
        });
    }

    private static ForecastInstance map(int byteLimit, ReplaceUtils.OperatorBuilder urlBuilder, ResultSet resultSet) throws SQLException, IOException {
        List<String> timeSeriesIdentifiers = new ArrayList<>();
        String tsIds = resultSet.getString(5);
        if (tsIds != null) {
            timeSeriesIdentifiers = Arrays.stream(tsIds.split("\n")).collect(toList());
        }
        Map<String, String> forecastInfo = new HashMap<>();
        String json = resultSet.getString(15);
        if (json != null) {
            forecastInfo = mapFromJson(json);
        }
        String officeId = resultSet.getString(4);
        String specId = resultSet.getString(1);
        String designator = resultSet.getString(3);
        Instant forecastDate = resultSet.getTimestamp(7, UTC_CALENDAR).toInstant();
        Instant issueDate = resultSet.getTimestamp(8, UTC_CALENDAR).toInstant();
        Timestamp earliesetTimestamp = resultSet.getTimestamp(13, UTC_CALENDAR);
        Timestamp latestTimestamp = resultSet.getTimestamp(14, UTC_CALENDAR);
        Instant firstDateTime = Optional.ofNullable(earliesetTimestamp).map(Timestamp::toInstant).orElse(null);
        Instant lastDateTime = Optional.ofNullable(latestTimestamp).map(Timestamp::toInstant).orElse(null);
        Struct blobFile = resultSet.getObject(10, Struct.class);
        byte[] fileData = null;
        String url = null;
        String fileName = null;
        String mediaType = null;
        if (blobFile != null) {
            Object[] attributes = blobFile.getAttributes();
            if (attributes != null) {
                fileName = (String) attributes[0];
                mediaType = (String) attributes[1];
                Blob blob = (Blob) attributes[4];
                if (blob.length() > byteLimit) {
                    String param = "&%s=%s";
                    String utf8 = "UTF-8";
                    url = urlBuilder.build().apply(specId) + "?"
                            + format(param, Controllers.NAME, URLEncoder.encode(specId, utf8))
                            + format(param, Controllers.FORECAST_DATE, URLEncoder.encode(forecastDate.toString(), utf8))
                            + format(param, Controllers.ISSUE_DATE, URLEncoder.encode(issueDate.toString(), utf8))
                            + format(param, Controllers.DESIGNATOR, URLEncoder.encode(designator, utf8))
                            + format(param, Controllers.OFFICE, URLEncoder.encode(officeId, utf8));
                } else {
                    try (InputStream is = blob.getBinaryStream()) {
                        fileData = BlobDao.readFully(is);
                    }
                }
            }
        }
        return new ForecastInstance.Builder()
                .withSpec(new ForecastSpec.Builder()
                        .withSpecId(specId)
                        .withDescription(resultSet.getString(2))
                        .withDesignator(designator)
                        .withOfficeId(officeId)
                        .withTimeSeriesIds(timeSeriesIdentifiers)
                        .withSourceEntityId(resultSet.getString(6))
                        .withLocationId(resultSet.getString(12))
                        .build())
                .withDateTime(forecastDate)
                .withIssueDateTime(issueDate)
                .withMaxAge(resultSet.getInt(9))
                .withFileDataUrl(url)
                .withFileData(fileData)
                .withFilename(fileName)
                .withFileMediaType(mediaType)
                .withNotes(resultSet.getString(11))
                .withFirstDateTime(firstDateTime)
                .withLastDateTime(lastDateTime)
                .withMetadata(forecastInfo)
                .build();
    }

    public ForecastInstance getForecastInstance(int byteLimit, ReplaceUtils.OperatorBuilder urlBuilder,
            String office, String name, String designator,
            Instant forecastDate, Instant issueDate) {
        String query = INSTANCE_QUERY + GET_ONE_CONDITIONS;
        return connectionResult(dsl, c -> {
            try (PreparedStatement preparedStatement = c.prepareStatement(query)) {
                preparedStatement.setString(1, office);
                preparedStatement.setString(2, name);
                preparedStatement.setString(3, designator);
                preparedStatement.setLong(4, forecastDate.toEpochMilli());
                preparedStatement.setLong(5, issueDate.toEpochMilli());
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return map(byteLimit, urlBuilder, resultSet);
                    } else {
                        String message = format("Could not find forecast instance for " +
                                        "office id: %s, spec id: %s, designator: %s, forecast date: %s, issue date: %s",
                                office, name, designator, forecastDate, issueDate);
                        throw new NotFoundException(message);
                    }
                }
            }
        });
    }

    public void update(ForecastInstance forecastInst) {
        String officeId = forecastInst.getSpec().getOfficeId();
        String specId = forecastInst.getSpec().getSpecId();
        String designator = forecastInst.getSpec().getDesignator();
        Instant forecastDate = forecastInst.getDateTime();
        Instant issueDate = forecastInst.getIssueDateTime();
        //Will throw a NotFoundException if instance doesn't exist
        ReplaceUtils.OperatorBuilder noopUrlBuilder = new ReplaceUtils.OperatorBuilder().withTemplate("")
                .withOperatorKey("{noop}");
        getForecastInstance(0, noopUrlBuilder, officeId, specId, designator, forecastDate, issueDate);
        create(forecastInst);
    }

    public void delete(String office, String name, String designator,
            Instant forecastDate, Instant issueDate) {
        connection(dsl, conn -> {
            setOffice(conn, office);
            DefaultBinding.THREAD_LOCAL.set(UTC_CALENDAR);
            CWMS_FCST_PACKAGE.call_DELETE_FCST(DSL.using(conn).configuration(), name, designator,
                    Timestamp.from(forecastDate), Timestamp.from(issueDate), "UTC", office);
        });
    }

    public void getFileBlob(String office, String name, String designator,
            Instant forecastDate, Instant issueDate, BlobDao.BlobConsumer consumer) {

        String query = FILE_QUERY + FILE_CONDITIONS;
        connection(dsl, c -> {
            try (PreparedStatement preparedStatement = c.prepareStatement(query)) {
                preparedStatement.setString(1, office);
                preparedStatement.setString(2, name);
                preparedStatement.setString(3, designator);
                preparedStatement.setLong(4, forecastDate.toEpochMilli());
                preparedStatement.setLong(5, issueDate.toEpochMilli());
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        Struct blobFile = resultSet.getObject(1, Struct.class);
                        if (blobFile != null) {
                            Object[] attributes = blobFile.getAttributes();
                            if (attributes != null) {
                                String mediaType = (String) attributes[1];
                                if (mediaType == null) {
                                    mediaType = "application/octet-stream";
                                }
                                Blob blob = (Blob) attributes[4];
                                consumer.accept(blob, mediaType);
                                return;
                            }
                        }
                    }
                    consumer.accept(null, null);
                }
            }
        });
    }
}
