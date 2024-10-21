package cwms.cda.data.dao.timeseriesprofile;

import static cwms.cda.data.dto.CwmsDTOPaginated.delimiter;
import static cwms.cda.data.dto.CwmsDTOPaginated.encodeCursor;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.countDistinct;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;
import static org.jooq.impl.DSL.using;
import static org.jooq.impl.DSL.val;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.DataColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesData;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record7;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;
import org.jooq.SelectSeekLimitStep;
import org.jooq.SelectSeekStep1;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_INST;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_INST_TSV2;
import usace.cwms.db.jooq.codegen.udt.records.PVQ_T;
import usace.cwms.db.jooq.codegen.udt.records.PVQ_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_REC_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_TAB_T;


public class TimeSeriesProfileInstanceDao extends JooqDao<TimeSeriesProfileInstance> {
    private static final Logger LOGGER = Logger.getLogger(TimeSeriesProfileInstanceDao.class.getName());
    private static final AV_TS_PROFILE_INST_TSV2 VIEW_TSV2 = AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2;
    private static final AV_TS_PROFILE_INST VIEW = AV_TS_PROFILE_INST.AV_TS_PROFILE_INST;

    public TimeSeriesProfileInstanceDao(DSLContext dsl) {
        super(dsl);
    }

    public void storeTimeSeriesProfileInstance(TimeSeriesProfile timeSeriesProfile, String profileData,
            Instant versionDate, String versionId, String storeRule, boolean overrideProtection) {
        connection(dsl, conn -> {
            setOffice(conn, timeSeriesProfile.getLocationId().getOfficeId());
            CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE__2(using(conn).configuration(),
                    timeSeriesProfile.getLocationId().getName(),
                    timeSeriesProfile.getKeyParameter(),
                    profileData,
                    versionId,
                    storeRule,
                    overrideProtection ? "T" : "F",
                    versionDate != null ? Timestamp.from(versionDate) : null,
                    timeSeriesProfile.getLocationId().getOfficeId());
        });
    }

    public void storeTimeSeriesProfileInstance(TimeSeriesProfileInstance timeseriesProfileInstance, String versionId,
            Instant versionInstant, String storeRule, String overrideProtection) {
        connection(dsl, conn -> {
            setOffice(conn, timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
            Map<String, BigInteger> parameterIdToCode = new HashMap<>();

            String parameter = timeseriesProfileInstance.getTimeSeriesProfile().getKeyParameter();
            BigDecimal parameterCodeDec = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_CODE(using(conn).configuration(),
                    parameter, timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
            parameterIdToCode.put(parameter, parameterCodeDec.toBigInteger());

            List<String> dependentParameters = timeseriesProfileInstance.getTimeSeriesProfile().getParameterList();
            for (String param : dependentParameters) {
                parameter = param;
                parameterCodeDec = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_CODE(using(conn).configuration(), parameter,
                        timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
                parameterIdToCode.put(parameter, parameterCodeDec.toBigInteger());
            }

            TS_PROF_DATA_T tsProfileData = new TS_PROF_DATA_T();
            tsProfileData.attach(using(conn).configuration());

            TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();

            STR_TAB_T units = new STR_TAB_T();
            for (Map.Entry<Long, List<TimeSeriesData>> entry
                    : timeseriesProfileInstance.getTimeSeriesList().entrySet()) {
                Long timestamp = entry.getKey();
                TS_PROF_DATA_REC_T dataRecord = new TS_PROF_DATA_REC_T();
                Timestamp timeStamp = Timestamp.from(Instant.ofEpochMilli(timestamp));
                dataRecord.setDATE_TIME(timeStamp);

                PVQ_TAB_T parameters = new PVQ_TAB_T();
                int n = 0;
                for (TimeSeriesData data : entry.getValue()) {
                    PVQ_T pvq = new PVQ_T();
                    String parameterId = timeseriesProfileInstance.getParameterColumns().get(n).getParameter();
                    BigInteger parameterCode = parameterIdToCode.get(parameterId);
                    pvq.setPARAMETER_CODE(parameterCode);
                    pvq.setVALUE(data.getValue());
                    pvq.setQUALITY_CODE(BigInteger.valueOf(data.getQuality()));
                    parameters.add(pvq);
                    if (!units.contains(timeseriesProfileInstance.getParameterColumns().get(n).getUnit())) {
                        units.add(timeseriesProfileInstance.getParameterColumns().get(n).getUnit());
                    }
                    n++;
                }
                dataRecord.setPARAMETERS(parameters);
                records.add(dataRecord);
            }

            BigDecimal locationCodeId = CWMS_LOC_PACKAGE.call_GET_LOCATION_CODE(using(conn).configuration(),
                    timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId(),
                    timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getName());

            tsProfileData.setRECORDS(records);
            tsProfileData.setLOCATION_CODE(locationCodeId.toBigInteger());
            tsProfileData.setTIME_ZONE("UTC");
            tsProfileData.setKEY_PARAMETER(parameterIdToCode.get(timeseriesProfileInstance
                    .getTimeSeriesProfile().getKeyParameter()));
            tsProfileData.setUNITS(units);

            Timestamp versionTimeStamp = Timestamp.from(versionInstant);

            CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE(using(conn).configuration(),
                    tsProfileData,
                    versionId,
                    storeRule,
                    overrideProtection,
                    versionTimeStamp,
                    timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
        });
    }


    public List<TimeSeriesProfileInstance> catalogTimeSeriesProfileInstances(String officeIdMask,
            String locationIdMask, String parameterIdMask, String versionMask) {

        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(
                VIEW.LOCATION_ID, locationIdMask);
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
                VIEW.OFFICE_ID, officeIdMask));
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
                VIEW.KEY_PARAMETER_ID, parameterIdMask));
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
                VIEW.VERSION_ID, versionMask));
        AV_TS_PROFILE profileView = AV_TS_PROFILE.AV_TS_PROFILE;
        @NotNull Result<Record> timeSeriesProfileInstanceResults = dsl.select(asterisk())
                .from(VIEW)
                .join(profileView)
                .on(VIEW.LOCATION_CODE.eq(profileView.LOCATION_CODE)
                        .and(VIEW.KEY_PARAMETER_CODE.eq(profileView.KEY_PARAMETER_CODE))
                        .and(VIEW.OFFICE_ID.eq(profileView.OFFICE_ID)))
                .where(whereCondition)
                .fetch();
        List<TimeSeriesProfileInstance> timeSeriesProfileInstanceList = new ArrayList<>();
        for (Record result : timeSeriesProfileInstanceResults) {
            // Get reference timeseries ID
            CwmsId tsCwmsId = null;
            Long tsId = result.get(profileView.REFERENCE_TS_CODE);
            if (tsId != null) {
                tsCwmsId = CwmsId.buildCwmsId(CWMS_TS_PACKAGE.call_GET_DB_OFFICE_ID(dsl.configuration(), tsId),
                        CWMS_TS_PACKAGE.call_GET_TS_ID(dsl.configuration(), tsId));
            }

            CwmsId locationId = new CwmsId.Builder()
                    .withOfficeId(result.get(VIEW.OFFICE_ID))
                    .withName(result.get(VIEW.LOCATION_ID))
                    .build();
            String parameterId = result.get(VIEW.KEY_PARAMETER_ID);
            TimeSeriesProfile timeSeriesProfile = new TimeSeriesProfile.Builder()
                    .withLocationId(locationId)
                    .withKeyParameter(parameterId)
                    .withReferenceTsId(tsCwmsId)
                    .build();
            TimeSeriesProfileInstance timeSeriesProfileInstance = new TimeSeriesProfileInstance.Builder()
                    .withTimeSeriesProfile(timeSeriesProfile)
                    .withVersion(result.get(VIEW.VERSION_ID))
                    .withVersionDate(result.get(VIEW.VERSION_DATE) != null
                            ? result.get(VIEW.VERSION_DATE).toInstant() : null)
                    .withFirstDate(result.get(VIEW.FIRST_DATE_TIME) != null
                            ? result.get(VIEW.FIRST_DATE_TIME).toInstant() : null)
                    .withLastDate(result.get(VIEW.LAST_DATE_TIME) != null
                            ? result.get(VIEW.LAST_DATE_TIME).toInstant() : null)
                    .build();

            timeSeriesProfileInstanceList.add(timeSeriesProfileInstance);
        }
        return timeSeriesProfileInstanceList;
    }

    public TimeSeriesProfileInstance retrieveTimeSeriesProfileInstance(CwmsId location, String keyParameter,
            String version,
            List<String> unit,
            Instant startTime,
            Instant endTime,
            String timeZone,
            boolean startInclusive,
            boolean endInclusive,
            boolean previous,
            boolean next,
            Instant versionDate,
            boolean maxVersion,
            String page,
            int pageSize) {

        Integer total = null;
        Timestamp tsCursor = null;
        String parameterId = null;

        if (versionDate != null && maxVersion) {
            throw new IllegalArgumentException("Cannot specify both version date and max version");
        }

        // Decode the cursor
        if (page != null && !page.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(page);

            LOGGER.fine("Decoded cursor");
            LOGGER.finest(() -> {
                StringBuilder sb = new StringBuilder();
                for (String part : parts) {
                    sb.append(part).append("\n");
                }
                return sb.toString();
            });

            if (parts.length > 1) {
                tsCursor = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(parts[0])));

                if (parts.length > 2) {
                    parameterId = parts[1];
                    total = Integer.parseInt(parts[2]);
                }
            }
        }

        Condition whereCondition;
        if (!maxVersion && versionDate != null) {
            // Build the where condition
            whereCondition = VIEW_TSV2.KEY_PARAMETER_ID.eq(keyParameter)
                    .and(VIEW_TSV2.LOCATION_ID.eq(location.getName())
                            .and(VIEW_TSV2.OFFICE_ID.eq(location.getOfficeId()))
                            .and(VIEW_TSV2.VERSION_ID.eq(version))
                            .and(VIEW_TSV2.VERSION_DATE.eq(Timestamp.from(versionDate))));
        } else {
            whereCondition = VIEW_TSV2.KEY_PARAMETER_ID.eq(keyParameter)
                    .and(VIEW_TSV2.LOCATION_ID.eq(location.getName())
                            .and(VIEW_TSV2.OFFICE_ID.eq(location.getOfficeId()))
                            .and(VIEW_TSV2.VERSION_ID.eq(version)));
        }

        // Add the unit conditions
        Condition unitCondition = VIEW_TSV2.UNIT_ID.eq(unit.get(0));
        for (int i = 1; i < unit.size(); i++) {
            unitCondition = unitCondition.or(VIEW_TSV2.UNIT_ID.eq(unit.get(i)));
        }
        whereCondition = whereCondition.and(unitCondition);

        // give the date time columns a name
        Field<Timestamp> endTimeCol = VIEW_TSV2.LAST_DATE_TIME;
        Field<Timestamp> startTimeCol = VIEW_TSV2.FIRST_DATE_TIME;
        Field<Timestamp> dateTimeCol = VIEW_TSV2.DATE_TIME;

        // handle previous flag
        if (previous) {
            Timestamp previousDateTime = null;
            SelectConditionStep<Record1<Timestamp>> prev = dsl.select(max(VIEW_TSV2.DATE_TIME))
                    .from(VIEW_TSV2)
                    .where(whereCondition.and(dateTimeCol.lessThan(Timestamp.from(startTime)))
                            .and(endTimeCol.greaterThan(Timestamp.from(startTime))));
            Record1<Timestamp> val = prev.fetchOne();

            if (val != null) {
                previousDateTime = val.value1();
                if (previousDateTime != null) {
                    startTime = previousDateTime.toInstant();
                    startInclusive = true;
                }
            }
        }

        // handle next flag
        if (next) {
            Timestamp nextDateTime = null;
            SelectConditionStep<Record1<Timestamp>> nex = dsl.select(min(VIEW_TSV2.DATE_TIME))
                    .from(VIEW_TSV2)
                    .where(whereCondition.and(dateTimeCol.greaterThan(Timestamp.from(endTime)))
                            .and(startTimeCol.le(Timestamp.from(endTime))));
            nextDateTime = Objects.requireNonNull(nex.fetchOne()).value1();
            if (nextDateTime != null) {
                endTime = nextDateTime.toInstant();
                endInclusive = true;
            }
        }

        // Add the time windows conditions depending on the inclusive flags
        if (startInclusive && endInclusive) {
            whereCondition = whereCondition
                    .and(VIEW_TSV2.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
                    .and(VIEW_TSV2.LAST_DATE_TIME.le(Timestamp.from(endTime)));
        } else if (!startInclusive && endInclusive) {
            whereCondition = whereCondition
                    .and(VIEW_TSV2.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
                    .and(VIEW_TSV2.LAST_DATE_TIME.le(Timestamp.from(endTime)));
        } else if (startInclusive) {
            whereCondition = whereCondition
                    .and(VIEW_TSV2.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
                    .and(VIEW_TSV2.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
        } else {
            whereCondition = whereCondition
                    .and(VIEW_TSV2.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
                    .and(VIEW_TSV2.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
        }
        Condition finalWhereCondition = whereCondition;

        // Get the total number of records if not already set
        if (total == null) {
            SelectHavingStep<Record1<Integer>> count = dsl.select(countDistinct(VIEW_TSV2.DATE_TIME))
                    .from(VIEW_TSV2)
                    .where(finalWhereCondition);
            total = Objects.requireNonNull(count.fetchOne()).value1();
        }

        // get total number of parameters to for setting fetch size
        SelectHavingStep<Record1<Integer>> count = dsl.select(countDistinct(VIEW_TSV2.PARAMETER_ID))
                .from(VIEW_TSV2)
                .where(finalWhereCondition);
        int totalPars = Objects.requireNonNull(count.fetchOne()).value1();

        // Get the max version date if needed
        Timestamp maxVersionDate = null;
        if (maxVersion) {
            SelectConditionStep<Record1<Timestamp>> maxVer = dsl.select(max(VIEW_TSV2.VERSION_DATE))
                    .from(VIEW_TSV2)
                    .where(finalWhereCondition);
            maxVersionDate = Objects.requireNonNull(maxVer.fetchOne()).value1();
        }
        Timestamp minVersionDate = null;
        if (!maxVersion && versionDate == null) {
            SelectConditionStep<Record1<Timestamp>> minVer = dsl.select(min(VIEW_TSV2.VERSION_DATE))
                    .from(VIEW_TSV2)
                    .where(finalWhereCondition);
            minVersionDate = Objects.requireNonNull(minVer.fetchOne()).value1();
        }

        // generate and run query to get the time series profile data
        Result<Record7<Double, Long, Timestamp, Long, Long, String, String>> result = null;
        SelectSeekStep1<Record7<Double, Long, Timestamp, Long, Long, String, String>, Timestamp> resultQuery = null;
        SelectConditionStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultCondQuery;
        SelectSeekLimitStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultQuery2 = null;
        if (pageSize != 0) {
            if (maxVersion) {
                resultCondQuery = dsl.select(VIEW_TSV2.VALUE,
                                VIEW_TSV2.QUALITY_CODE,
                                VIEW_TSV2.DATE_TIME,
                                VIEW_TSV2.LOCATION_CODE,
                                VIEW_TSV2.KEY_PARAMETER_CODE,
                                VIEW_TSV2.PARAMETER_ID,
                                VIEW_TSV2.UNIT_ID)
                        .from(VIEW_TSV2)
                        .where(finalWhereCondition.and(VIEW_TSV2.VERSION_DATE.eq(maxVersionDate)));
            } else if (versionDate == null) {
                resultCondQuery = dsl.select(VIEW_TSV2.VALUE,
                                VIEW_TSV2.QUALITY_CODE,
                                VIEW_TSV2.DATE_TIME,
                                VIEW_TSV2.LOCATION_CODE,
                                VIEW_TSV2.KEY_PARAMETER_CODE,
                                VIEW_TSV2.PARAMETER_ID,
                                VIEW_TSV2.UNIT_ID)
                        .from(VIEW_TSV2)
                        .where(finalWhereCondition.and(VIEW_TSV2.VERSION_DATE.eq(minVersionDate)));
            } else {
                resultCondQuery = dsl.select(VIEW_TSV2.VALUE,
                                VIEW_TSV2.QUALITY_CODE,
                                VIEW_TSV2.DATE_TIME,
                                VIEW_TSV2.LOCATION_CODE,
                                VIEW_TSV2.KEY_PARAMETER_CODE,
                                VIEW_TSV2.PARAMETER_ID,
                                VIEW_TSV2.UNIT_ID)
                        .from(VIEW_TSV2)
                        .where(finalWhereCondition);
            }

            // If there is a cursor, use it with the JOOQ seek method
            // Needs the parameter and cursor of the record before the first one on the next page
            //     to correctly split the data into pages
            // Searches for matching records based on the cursor to start the next page's results
            if (tsCursor == null) {
                resultQuery = resultCondQuery.and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
                                .call_TO_TIMESTAMP__2(val(startTime.toEpochMilli()))))
                        .and(dateTimeCol.lessOrEqual(CWMS_UTIL_PACKAGE
                                .call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
                        .orderBy(VIEW_TSV2.DATE_TIME);

            } else {
                resultQuery2 = resultCondQuery
                        .and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
                                .call_TO_TIMESTAMP__2(val(tsCursor.toInstant().toEpochMilli()))))
                        .and(dateTimeCol.lessOrEqual(CWMS_UTIL_PACKAGE
                                .call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
                        .orderBy(VIEW_TSV2.DATE_TIME, VIEW_TSV2.PARAMETER_ID)
                        .seek(tsCursor, parameterId);
            }

            // Get the results
            // if the page number is set, limit the results to the page size
            if (pageSize > 0) {
                int fetchSize = pageSize * totalPars;
                if (tsCursor == null) {
                    result = resultQuery.limit(fetchSize).fetch();
                } else {
                    result = resultQuery2.limit(fetchSize).fetch();
                }
            } else {
                if (tsCursor == null) {
                    result = resultQuery.fetch();
                } else {
                    result = resultQuery2.fetch();
                }
            }
            Result<?> lastRecord = result;
            LOGGER.fine(lastRecord::toString);
        }

        // Throw 404 if no results
        if (result == null || result.isEmpty()) {
            throw new NotFoundException("No time series profile data found for the given parameters");
        }

        // map the results to a TimeSeriesProfileInstance
        Result<?> finalResult = result;

        BigInteger locationCode = null;
        BigInteger keyParameterCode = null;

        TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();
        Map<Timestamp, Map<String, PVQ_T>> timeValuePairMap = new TreeMap<>();
        Map<String, String> unitParamMap = new TreeMap<>();

        // boolean to keep track of data that is consistent across all records
        boolean parentData = false;
        for (Record resultRecord : finalResult) {
            if (!parentData) {
                locationCode = BigInteger.valueOf(resultRecord.get(VIEW_TSV2.LOCATION_CODE));
                keyParameterCode = BigInteger.valueOf(resultRecord.get(VIEW_TSV2.KEY_PARAMETER_CODE));
                parentData = true;
            }

            // map the unit to the parameter
            if (unitParamMap.get(resultRecord.get(VIEW_TSV2.PARAMETER_ID)) == null) {
                unitParamMap.put(resultRecord.get(VIEW_TSV2.PARAMETER_ID),
                        resultRecord.get(VIEW_TSV2.UNIT_ID));
            }

            // map the parameter, TVQ data
            Timestamp dateTime = resultRecord.get(VIEW_TSV2.DATE_TIME);
            Map<String, PVQ_T> dataMap;
            if (timeValuePairMap.get(dateTime) == null) {
                dataMap = new TreeMap<>();
            } else {
                dataMap = timeValuePairMap.get(dateTime);
            }
            dataMap.put(resultRecord.get(VIEW_TSV2.PARAMETER_ID), new PVQ_T(keyParameterCode,
                    resultRecord.get(VIEW_TSV2.VALUE),
                    BigInteger.valueOf(resultRecord.get(VIEW_TSV2.QUALITY_CODE))));
            timeValuePairMap.put(dateTime, dataMap);
        }

        // map the value/quality data to the timestamp
        int index = 0;
        for (Map.Entry<Timestamp, Map<String, PVQ_T>> entry : timeValuePairMap.entrySet()) {
            PVQ_TAB_T parameters = new PVQ_TAB_T();
            for (Map.Entry<String, PVQ_T> value : entry.getValue().entrySet()) {
                parameters.add(value.getValue());
            }
            records.add(index, new TS_PROF_DATA_REC_T(entry.getKey(), parameters));
            index++;
        }

        if (minVersionDate != null) {
            versionDate = minVersionDate.toInstant();
        } else if (maxVersionDate != null) {
            versionDate = maxVersionDate.toInstant();
        }

        List<ParameterColumnInfo> paramList = new ArrayList<>();
        int ordinal = 1;
        for (Map.Entry<String, String> entry : unitParamMap.entrySet()) {
            ParameterColumnInfo parameterColumnInfo = new ParameterColumnInfo.Builder()
                    .withParameter(entry.getKey())
                    .withOrdinal(ordinal)
                    .withUnit(entry.getValue())
                    .build();
            ordinal++;
            paramList.add(parameterColumnInfo);
        }
        Map<Long, List<TimeSeriesData>> timeSeriesProfileInstanceList = new TreeMap<>();

        // map the TVQ data to the TimeSeriesProfileInstance
        // needs previous parameter and cursor to be set to correctly split the data into pages
        // adds page, nextpage data to the TimeSeriesProfileInstance
        for (Map.Entry<Timestamp, Map<String, PVQ_T>> entry : timeValuePairMap.entrySet()) {
            for (Map.Entry<String, PVQ_T> dataValue : entry.getValue().entrySet()) {
                Timestamp dateTime = entry.getKey();
                if (timeSeriesProfileInstanceList.containsKey(dateTime.getTime())) {
                    timeSeriesProfileInstanceList.get(dateTime.getTime())
                        .add(new TimeSeriesData(dataValue.getValue().getVALUE(),
                            dataValue.getValue().getQUALITY_CODE().intValue()));
                } else {
                    List<TimeSeriesData> dataList = new ArrayList<>();
                    dataList.add(new TimeSeriesData(dataValue.getValue().getVALUE(),
                            dataValue.getValue().getQUALITY_CODE().intValue()));
                    timeSeriesProfileInstanceList.put(dateTime.getTime(), dataList);
                }
            }
        }

        // add null values to the TimeSeriesProfileInstance value list if
        // the data is missing for the associated parameter
        for (Map.Entry<Long, List<TimeSeriesData>> entry : timeSeriesProfileInstanceList.entrySet()) {
            if (entry.getValue().size() < paramList.size()) {
                for (int i = 0; i < paramList.size(); i++) {
                    Timestamp dateTime = Timestamp.from(Instant.ofEpochMilli(entry.getKey()));
                    boolean inBounds = i < entry.getValue().size();
                    if (!inBounds) {
                        timeSeriesProfileInstanceList.get(dateTime.getTime()).add(i, null);
                        continue;
                    }
                    if (timeValuePairMap.get(dateTime) == null
                            || !timeValuePairMap.get(dateTime).containsKey(paramList.get(i).getParameter())) {
                        timeSeriesProfileInstanceList.get(dateTime.getTime()).add(i, null);
                    }
                }
            }
        }

        STR_TAB_T units = new STR_TAB_T(unit);

        TS_PROF_DATA_T timeSeriesProfileData = new TS_PROF_DATA_T(locationCode, keyParameterCode,
                timeZone, units, records);

        // Get reference timeseries ID
        AV_TS_PROFILE profileView = AV_TS_PROFILE.AV_TS_PROFILE;
        CwmsId tsCwmsId = null;

        Record1<Long> val = dsl.selectDistinct(profileView.REFERENCE_TS_CODE)
                .from(profileView)
                .join(VIEW)
                .on(profileView.LOCATION_CODE.eq(VIEW.LOCATION_CODE)
                        .and(profileView.KEY_PARAMETER_CODE.eq(VIEW.KEY_PARAMETER_CODE))
                        .and(profileView.OFFICE_ID.eq(VIEW.OFFICE_ID)))
                .where(profileView.KEY_PARAMETER_ID.eq(keyParameter)
                        .and(profileView.LOCATION_ID.eq(location.getName())
                                .and(profileView.OFFICE_ID.eq(location.getOfficeId()))))
                .fetchOne();
        if (val != null) {
            Long tsId = val.value1();
            if (tsId != null) {
                tsCwmsId = CwmsId.buildCwmsId(CWMS_TS_PACKAGE.call_GET_DB_OFFICE_ID(dsl.configuration(), tsId),
                        CWMS_TS_PACKAGE.call_GET_TS_ID(dsl.configuration(), tsId));
            }
        }

        // map the TimeSeriesProfileInstance without the value/quality data
        return map(location.getOfficeId(), location.getName(), keyParameter,
                timeSeriesProfileData, version, versionDate, startTime, endTime, unitParamMap,
                pageSize, total, paramList, timeSeriesProfileInstanceList, tsCwmsId);
    }

    public void deleteTimeSeriesProfileInstance(CwmsId location, String keyParameter,
            String version, Instant firstDate, String timeZone, boolean overrideProtection, Instant versionDate) {

        connection(dsl, conn -> {
            setOffice(conn, location.getOfficeId());
            Timestamp versionTimestamp = null;
            if (versionDate != null) {
                versionTimestamp = Timestamp.from(versionDate);
            }

            CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE_INSTANCE(
                    using(conn).configuration(),
                    location.getName(),
                    keyParameter,
                    version,
                    Timestamp.from(firstDate),
                    timeZone,
                    overrideProtection ? "T" : "F",
                    versionTimestamp,
                    location.getOfficeId()
            );
        });
    }


    private TimeSeriesProfileInstance map(String officeId, String location, String keyParameter,
            TS_PROF_DATA_T timeSeriesProfileData, String version, Instant versionDate, Instant startTime,
            Instant endTime, Map<String, String> unitParamMap, int pageSize, int total,
            List<ParameterColumnInfo> parameterColumnInfoList,
            Map<Long, List<TimeSeriesData>> timeSeriesProfileInstanceList, CwmsId tsId) {
        List<Instant> timeList = new ArrayList<>();
        List<DataColumnInfo> dataColumnInfoList = new ArrayList<>();
        DataColumnInfo valueDataColumnInfo = new DataColumnInfo.Builder()
                .withName("value")
                .withOrdinal(1)
                .withDataType(Double.class.getName())
                .build();
        DataColumnInfo qualityDataColumnInfo = new DataColumnInfo.Builder()
                .withName("quality")
                .withOrdinal(2)
                .withDataType(Integer.class.getName())
                .build();
        dataColumnInfoList.add(valueDataColumnInfo);
        dataColumnInfoList.add(qualityDataColumnInfo);
        TS_PROF_DATA_TAB_T records = timeSeriesProfileData.getRECORDS();
        for (TS_PROF_DATA_REC_T dataRecord : records) {
            Timestamp dateTime = dataRecord.getDATE_TIME();
            timeList.add(dateTime.toInstant());
        }

        List<String> parameterList = new ArrayList<>(unitParamMap.keySet());
        CwmsId locationId = new CwmsId.Builder()
                .withOfficeId(officeId)
                .withName(location)
                .build();
        TimeSeriesProfile timeSeriesProfile = new TimeSeriesProfile.Builder()
                .withKeyParameter(keyParameter)
                .withLocationId(locationId)
                .withParameterList(parameterList)
                .withReferenceTsId(tsId)
                .build();

        String nextPage = null;
        Long latestTimestamp = timeSeriesProfileInstanceList.keySet().stream().max(Long::compare).orElse(null);

        if (timeSeriesProfileInstanceList.keySet().size() >= pageSize && total > pageSize && latestTimestamp != null) {
            nextPage = encodeCursor(delimiter, String.format("%d", latestTimestamp),
                    parameterColumnInfoList.get(findParameterIndex(parameterColumnInfoList, latestTimestamp,
                            timeSeriesProfileInstanceList)).getParameter(), total);
        }

        Long earliestTimestamp = timeSeriesProfileInstanceList.keySet().stream().min(Long::compare).orElse(null);
        String timeZone = timeSeriesProfileData.getTIME_ZONE();

        TimeSeriesProfileInstance.Builder builder = new TimeSeriesProfileInstance.Builder();
        builder.withTimeSeriesProfile(timeSeriesProfile);
        builder.withTimeSeriesList(timeSeriesProfileInstanceList);
        builder.withVersion(version);
        builder.withFirstDate(startTime);
        builder.withLastDate(endTime);
        builder.withLocationTimeZone(timeZone);
        builder.withPage(encodeCursor(delimiter, String.format("%d", earliestTimestamp),
                keyParameter, total));
        builder.withPageSize(pageSize);
        builder.withNextPage(nextPage);
        builder.withTotal(total);
        builder.withDataColumns(dataColumnInfoList);
        builder.withParameterColumns(parameterColumnInfoList);
        builder.withPageFirstDate(timeList.stream().min(Instant::compareTo).orElse(null));
        builder.withPageLastDate(timeList.stream().max(Instant::compareTo).orElse(null));
        builder.withVersionDate(versionDate);
        return builder
                .build();

    }

    private static int findParameterIndex(List<ParameterColumnInfo> parameterColumnInfoList, long latestTime,
            Map<Long, List<TimeSeriesData>> timeSeriesProfileInstanceList) {
        int returnIndex = -1;
        int index = 0;
        for (int i = 0; i < parameterColumnInfoList.size(); i++) {
            if (timeSeriesProfileInstanceList.get(latestTime).get(index) != null) {
                returnIndex = index;
            }
            index++;
        }
        return returnIndex;
    }
}