package cwms.cda.data.dao.timeseriesprofile;

import static org.jooq.impl.DSL.*;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.DataColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterColumnInfo;
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
import org.jooq.SelectSeekLimitStep;
import org.jooq.SelectSeekStep1;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_INST;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_INST_TSV2;
import usace.cwms.db.jooq.codegen.udt.records.PVQ_T;
import usace.cwms.db.jooq.codegen.udt.records.PVQ_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_REC_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROF_DATA_TAB_T;




public class TimeSeriesProfileInstanceDao extends JooqDao<TimeSeriesProfileInstance> {
	private static final Logger logger = Logger.getLogger(TimeSeriesProfileInstanceDao.class.getName());
	private static final AV_TS_PROFILE_INST_TSV2 cwmsTsInstView = AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2;

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
			Instant versionInstant, String storeRule,String overrideProtection) {
		connection(dsl, conn -> {
			setOffice(conn, timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
			BigDecimal locationCodeId = CWMS_LOC_PACKAGE.call_GET_LOCATION_CODE(using(conn).configuration(),
					timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId(),
					timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getName());


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

			for (Long timestamp : timeseriesProfileInstance.getTimeSeriesList().keySet()) {
				TS_PROF_DATA_REC_T dataRecord = new TS_PROF_DATA_REC_T();
				Timestamp timeStamp =  Timestamp.from(Instant.ofEpochMilli(timestamp));
				dataRecord.setDATE_TIME(timeStamp);
				records.add(dataRecord);
			}
			STR_TAB_T units = new STR_TAB_T();
			for (TS_PROF_DATA_REC_T ts_prof_data_rec_t : records) {
				PVQ_TAB_T parameters = new PVQ_TAB_T();
				for (int i = 0; i < timeseriesProfileInstance.getTimeSeriesList().size(); i++) {
					PVQ_T pvq = new PVQ_T();
					String parameterId = timeseriesProfileInstance.getParameterColumns().get(i).getParameter();
					BigInteger parameterCode = parameterIdToCode.get(parameterId);
					pvq.setPARAMETER_CODE(parameterCode);
					parameters.add(pvq);
					units.add(timeseriesProfileInstance.getParameterColumns().get(i).getUnit());
				}
				ts_prof_data_rec_t.setPARAMETERS(parameters);
			}

			int i = 0;
			for (Long timestamp : timeseriesProfileInstance.getTimeSeriesList().keySet()) {
				for (int j = 0; j < timeseriesProfileInstance.getTimeSeriesList().get(timestamp).size(); j++) {
					TS_PROF_DATA_REC_T dataRecord = records.get(j);
					dataRecord.getPARAMETERS().get(i).setVALUE(
						timeseriesProfileInstance.getTimeSeriesList().get(timestamp).get(j).getValue());
				}
				i++;
			}

			tsProfileData.setRECORDS(records);
			tsProfileData.setLOCATION_CODE(locationCodeId.toBigInteger());
			tsProfileData.setTIME_ZONE("UTC");
			tsProfileData.setKEY_PARAMETER(parameterIdToCode.get(timeseriesProfileInstance.getTimeSeriesProfile().getKeyParameter()));
			tsProfileData.setUNITS(units);

			Timestamp versionTimeStamp =  Timestamp.from(versionInstant);

				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE(using(conn).configuration(),
						tsProfileData,
						versionId,
						storeRule,
						overrideProtection,
						versionTimeStamp,
						timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
				}
			);
	}


	public List<TimeSeriesProfileInstance> catalogTimeSeriesProfileInstances( String officeIdMask, String locationIdMask, String parameterIdMask, String versionMask)
	{
		List<TimeSeriesProfileInstance> timeSeriesProfileInstanceList = new ArrayList<>();

		Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.LOCATION_ID, locationIdMask);
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.OFFICE_ID, officeIdMask));
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.KEY_PARAMETER_ID, parameterIdMask));
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.VERSION_ID, versionMask));

		@NotNull Result<Record> timeSeriesProfileInstanceResults =  dsl.select(asterisk()).from(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST)
				.where(whereCondition)
				.fetch();
		for (Record result : timeSeriesProfileInstanceResults) {
				CwmsId locationId = new CwmsId.Builder()
						.withOfficeId(result.get("OFFICE_ID",String.class))
						.withName(result.get("LOCATION_ID", String.class))
						.build();
				String parameterId = result.get("KEY_PARAMETER_ID", String.class);
				TimeSeriesProfile timeSeriesProfile = new TimeSeriesProfile.Builder()
						.withLocationId(locationId)
						.withKeyParameter(parameterId)
						.build();
				TimeSeriesProfileInstance timeSeriesProfileInstance = new TimeSeriesProfileInstance.Builder()
						.withTimeSeriesProfile(timeSeriesProfile)
						.withVersion(result.get("VERSION_ID", String.class))
						.withVersionDate(result.get("VERSION_DATE", Instant.class))
						.withFirstDate(result.get("FIRST_DATE_TIME", Instant.class))
						.withLastDate(result.get("LAST_DATE_TIME", Instant.class))
						.build();

			timeSeriesProfileInstanceList.add(timeSeriesProfileInstance);
		}
		return timeSeriesProfileInstanceList;
	}

	public TimeSeriesProfileInstance retrieveTimeSeriesProfileInstance(CwmsId location, String keyParameter,
		String version,
		String[] unit,
		Instant startTime,
		Instant endTime,
		String timeZone,
		String startInclusive,
		String endInclusive,
		String previous,
		String next,
		Instant versionDate,
		String maxVersion,
		String page,
		int pageSize)
	{
		Integer total = null;
		String cursor = null;
		Timestamp tsCursor = null;
		String parameterId = null;

		if (page != null && !page.isEmpty()) {
			final String[] parts = CwmsDTOPaginated.decodeCursor(page);

			logger.fine("Decoded cursor");
			logger.finest(() -> {
				StringBuilder sb = new StringBuilder();
				for (String part : parts) {
					sb.append(part).append("\n");
				}
				return sb.toString();
			});

			if (parts.length > 1) {
				cursor = parts[0];
				tsCursor = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(parts[0])));

				if (parts.length > 2) {
					parameterId = parts[1];
					total = Integer.parseInt(parts[2]);
				}

				pageSize = Integer.parseInt(parts[parts.length - 1]);
			}
		}

		Condition whereCondition = cwmsTsInstView.KEY_PARAMETER_ID.eq(keyParameter)
				.and(cwmsTsInstView.LOCATION_ID.eq(location.getName())
				.and(cwmsTsInstView.OFFICE_ID.eq(location.getOfficeId()))
				.and(cwmsTsInstView.VERSION_ID.eq(version))
				.and(cwmsTsInstView.VERSION_DATE.eq(Timestamp.from(versionDate))));

        Condition unitCondition = cwmsTsInstView.UNIT_ID.eq(unit[0]);
        for(int i = 1; i < unit.length; i++) {
            unitCondition = unitCondition.or(cwmsTsInstView.UNIT_ID.eq(unit[i]));
        }
        whereCondition = whereCondition.and(unitCondition);

        if(OracleTypeMap.parseBool(startInclusive) && OracleTypeMap.parseBool(endInclusive)){
			whereCondition = whereCondition
					.and(cwmsTsInstView.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
					.and(cwmsTsInstView.LAST_DATE_TIME.le(Timestamp.from(endTime)));
		} else if (!OracleTypeMap.parseBool(startInclusive) && (OracleTypeMap.parseBool(endInclusive))) {
			whereCondition = whereCondition
					.and(cwmsTsInstView.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
					.and(cwmsTsInstView.LAST_DATE_TIME.le(Timestamp.from(endTime)));
		} else if (OracleTypeMap.parseBool(startInclusive) && !OracleTypeMap.parseBool(endInclusive)) {
			whereCondition = whereCondition
					.and(cwmsTsInstView.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
					.and(cwmsTsInstView.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		} else {
			whereCondition = whereCondition
					.and(cwmsTsInstView.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
					.and(cwmsTsInstView.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		}
		Condition finalWhereCondition = whereCondition;

		// Give the date time column a name
		Field<Timestamp> dateTimeCol = field("DATE_TIME", Timestamp.class).as("DATE_TIME");

		final String recordCursor = cursor;
		final int recordPageSize = pageSize;


		if (total == null) {
			SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
					.from(cwmsTsInstView)
					.where(finalWhereCondition);
			total = count.fetchOne().value1();
		}
		Result<Record7<Double, Long, Timestamp, Long, Long, String, String>> result = null;
		SelectSeekStep1<Record7<Double, Long, Timestamp, Long, Long, String, String>, Timestamp> resultQuery = null;
		SelectConditionStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultCondQuery = null;
		SelectSeekLimitStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultQuery2 = null;
		if (pageSize != 0) {
			resultCondQuery = dsl.select(cwmsTsInstView.VALUE,
							cwmsTsInstView.QUALITY_CODE,
							cwmsTsInstView.DATE_TIME,
							cwmsTsInstView.LOCATION_CODE,
							cwmsTsInstView.KEY_PARAMETER_CODE,
							cwmsTsInstView.PARAMETER_ID,
							cwmsTsInstView.UNIT_ID)
					.from(cwmsTsInstView)
					.where(finalWhereCondition);

			if (tsCursor == null) {
				resultQuery = resultCondQuery.and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(startTime.toEpochMilli()))))
						.and(dateTimeCol.lessThan(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
						.orderBy(cwmsTsInstView.DATE_TIME);

			} else {
				resultQuery2 = resultCondQuery
						.and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(tsCursor.toInstant().toEpochMilli()))))
						.and(dateTimeCol.lessThan(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
						.orderBy(cwmsTsInstView.DATE_TIME, cwmsTsInstView.PARAMETER_ID)
						.seek(tsCursor, parameterId);

			}

			if (pageSize > 0) {
				if (tsCursor == null) {
					result = resultQuery.limit(pageSize + 1).fetch();
				} else {
					result = resultQuery2.limit(pageSize + 1).fetch();
				}
			} else {
				if (tsCursor == null) {
					result = resultQuery.fetch();
				} else {
					result = resultQuery2.fetch();
				}
			}
			Result<?> lastRecord = result;
			logger.fine(lastRecord::toString);
		}

		if (result == null || result.isEmpty()) {
			throw new NotFoundException("No time series profile data found for the given parameters");
		}

		Result<?> finalResult = result;
		int totalRecords = total;
		TS_PROF_DATA_T timeSeriesProfileData;
		BigInteger locationCode = null;
		BigInteger keyParameterCode = null;
		STR_TAB_T units = new STR_TAB_T(unit);
		TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();
		Map<Timestamp, PVQ_TAB_T> timeValuePairMap = new TreeMap<>();
		Map<Timestamp, Map<Double, String>> parameterTimeMap = new TreeMap<>();
		Map<String, String> unitParamMap = new TreeMap<>();

		boolean parentData = false;
		for (Record resultRecord : finalResult) {
			if (!parentData) {
				locationCode = resultRecord.get("LOCATION_CODE", BigInteger.class);
				keyParameterCode = resultRecord.get("KEY_PARAMETER_CODE", BigInteger.class);
				parentData = true;
			}
			if (unitParamMap.get(resultRecord.get("PARAMETER_ID", String.class)) == null) {
				unitParamMap.put(resultRecord.get("PARAMETER_ID", String.class), resultRecord.get("UNIT_ID", String.class));
			}
			Timestamp dateTime = resultRecord.get("DATE_TIME", Timestamp.class);
			if (timeValuePairMap.get(dateTime) != null) {
				PVQ_TAB_T existingParameters = timeValuePairMap.get(dateTime);
				existingParameters.add(new PVQ_T(keyParameterCode, resultRecord.get("VALUE", Double.class),
						resultRecord.get("QUALITY_CODE", BigInteger.class)));
				timeValuePairMap.put(dateTime, existingParameters);
				Map<Double, String> existingParameterList = parameterTimeMap.get(dateTime);
				existingParameterList.put(resultRecord.get("VALUE", Double.class), resultRecord.get("PARAMETER_ID", String.class));
				parameterTimeMap.put(dateTime, existingParameterList);
			} else {
				PVQ_TAB_T parameters = new PVQ_TAB_T(new PVQ_T(keyParameterCode, resultRecord.get("VALUE", Double.class),
						resultRecord.get("QUALITY_CODE", BigInteger.class)));
				timeValuePairMap.put(dateTime, parameters);
				Map<Double, String> parameterList = new TreeMap<>();
				parameterList.put(resultRecord.get("VALUE", Double.class), resultRecord.get("PARAMETER_ID", String.class));
				parameterTimeMap.put(dateTime, parameterList);
			}
		}
		int index = 0;
		for (Map.Entry<Timestamp, PVQ_TAB_T> entry : timeValuePairMap.entrySet()) {
			records.add(index, new TS_PROF_DATA_REC_T(entry.getKey(), entry.getValue()));
			index++;
		}
		timeSeriesProfileData = new TS_PROF_DATA_T(locationCode, keyParameterCode, timeZone, units, records);

		TimeSeriesProfileInstance returnInstance = map(location.getOfficeId(), location.getName(), keyParameter, timeSeriesProfileData, version,
				versionDate, startTime, endTime, unitParamMap, recordCursor, recordPageSize, totalRecords);

		String previousParameter = null;
		Timestamp previousCursor = Timestamp.from(startTime);
		for (Map.Entry<Timestamp, PVQ_TAB_T> entry : timeValuePairMap.entrySet()) {
			for (PVQ_T pvq : entry.getValue()) {
				Timestamp dateTime = entry.getKey();
				returnInstance.addValue(dateTime, pvq.getVALUE(), pvq.getQUALITY_CODE().intValue(), previousParameter, previousCursor);
				previousCursor = dateTime;
				previousParameter = parameterTimeMap.get(dateTime).get(pvq.getVALUE());
			}
		}

		return returnInstance;
	}

	public void deleteTimeSeriesProfileInstance(CwmsId location, String keyParameter,
			String version, Instant firstDate, String timeZone,boolean overrideProtection, Instant versionDate)
	{
		connection(dsl, conn -> {
			setOffice(conn, location.getOfficeId());
			Timestamp versionTimestamp = null;
			if(versionDate!=null) {
				versionTimestamp = Timestamp.from(versionDate);
			}

				CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE_INSTANCE(
						using(conn).configuration(),
						location.getName(),
						keyParameter,
						version,
						Timestamp.from(firstDate),
						timeZone,
						overrideProtection?"T":"F",
						versionTimestamp,
						location.getOfficeId()
				);

		});
	}


	private TimeSeriesProfileInstance map(String officeId, String location, String keyParameter,
			TS_PROF_DATA_T timeSeriesProfileData, String version, Instant versionDate, Instant startTime,
			Instant endTime, Map<String, String> unitParamMap, String page, int pageSize, int total)  {
		String timeZone = timeSeriesProfileData.getTIME_ZONE();
		TS_PROF_DATA_TAB_T records = timeSeriesProfileData.getRECORDS();
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

		for (TS_PROF_DATA_REC_T dataRecord : records) {
			Timestamp dateTime = dataRecord.getDATE_TIME();
			timeList.add(dateTime.toInstant());
		}

		List<ParameterColumnInfo> parameterColumnInfoList = new ArrayList<>();
		int ordinal = 1;
		for (Map.Entry<String, String> entry : unitParamMap.entrySet()) {
			ParameterColumnInfo parameterColumnInfo = new ParameterColumnInfo.Builder()
					.withParameter(entry.getKey())
					.withOrdinal(ordinal)
					.withUnit(entry.getValue())
					.build();
			ordinal++;
			parameterColumnInfoList.add(parameterColumnInfo);
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
				.build();
		return new TimeSeriesProfileInstance.Builder()
				.withTimeSeriesProfile(timeSeriesProfile)
				.withTimeSeriesList(new TreeMap<>())
				.withVersion(version)
				.withFirstDate(startTime)
				.withLastDate(endTime)
				.withLocationTimeZone(timeZone)
				.withPage(page)
				.withPageSize(pageSize)
				.withTotal(total)
				.withDataColumns(dataColumnInfoList)
				.withParameterColumns(parameterColumnInfoList)
				.withPageFirstDate(timeList.stream().min(Instant::compareTo).orElse(null))
				.withPageLastDate(timeList.stream().max(Instant::compareTo).orElse(null))
				.withVersionDate(versionDate)
				.build();
	}
}