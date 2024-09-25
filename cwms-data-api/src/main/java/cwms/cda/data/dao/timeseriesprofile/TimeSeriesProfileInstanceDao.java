package cwms.cda.data.dao.timeseriesprofile;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
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
			tsProfileData.setKEY_PARAMETER(parameterIdToCode.get(timeseriesProfileInstance
					.getTimeSeriesProfile().getKeyParameter()));
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


	public List<TimeSeriesProfileInstance> catalogTimeSeriesProfileInstances(String officeIdMask,
			String locationIdMask, String parameterIdMask, String versionMask)
	{
		List<TimeSeriesProfileInstance> timeSeriesProfileInstanceList = new ArrayList<>();

		Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(
				AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.LOCATION_ID, locationIdMask);
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
				AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.OFFICE_ID, officeIdMask));
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
				AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.KEY_PARAMETER_ID, parameterIdMask));
		whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(
				AV_TS_PROFILE_INST.AV_TS_PROFILE_INST.VERSION_ID, versionMask));

		@NotNull Result<Record> timeSeriesProfileInstanceResults =  dsl.select(asterisk())
				.from(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST)
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
		int pageSize)
	{
		Integer total = null;
		String cursor = null;
		Timestamp tsCursor = null;
		String parameterId = null;

		if (versionDate != null && maxVersion) {
			throw new IllegalArgumentException("Cannot specify both version date and max version");
		}

		// Decode the cursor
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
			}
		}

		Condition whereCondition;
		if (!maxVersion && versionDate != null) {
			// Build the where condition
			whereCondition = cwmsTsInstView.KEY_PARAMETER_ID.eq(keyParameter)
					.and(cwmsTsInstView.LOCATION_ID.eq(location.getName())
							.and(cwmsTsInstView.OFFICE_ID.eq(location.getOfficeId()))
							.and(cwmsTsInstView.VERSION_ID.eq(version))
							.and(cwmsTsInstView.VERSION_DATE.eq(Timestamp.from(versionDate))));
		}
		else  {
			whereCondition = cwmsTsInstView.KEY_PARAMETER_ID.eq(keyParameter)
					.and(cwmsTsInstView.LOCATION_ID.eq(location.getName())
							.and(cwmsTsInstView.OFFICE_ID.eq(location.getOfficeId()))
							.and(cwmsTsInstView.VERSION_ID.eq(version)));
		}

		// Add the unit conditions
        Condition unitCondition = cwmsTsInstView.UNIT_ID.eq(unit.get(0));
        for (int i = 1; i < unit.size(); i++) {
            unitCondition = unitCondition.or(cwmsTsInstView.UNIT_ID.eq(unit.get(i)));
        }
        whereCondition = whereCondition.and(unitCondition);

		// give the date time columns a name
		Field<Timestamp> endTimeCol = field("LAST_DATE_TIME", Timestamp.class).as("LAST_DATE_TIME");
		Field<Timestamp> startTimeCol = field("FIRST_DATE_TIME", Timestamp.class).as("FIRST_DATE_TIME");
		Field<Timestamp> dateTimeCol = field("DATE_TIME", Timestamp.class).as("DATE_TIME");

		// handle previous flag
		if (previous) {
			Timestamp previousDateTime = null;
			SelectConditionStep<Record1<Timestamp>> prev = dsl.select(max(cwmsTsInstView.DATE_TIME))
					.from(cwmsTsInstView)
					.where(whereCondition.and(dateTimeCol.lessThan(Timestamp.from(startTime)))
							.and(endTimeCol.greaterThan(Timestamp.from(startTime))));
			previousDateTime = prev.fetchOne().value1();
			if (previousDateTime != null) {
				startTime = previousDateTime.toInstant();
				startInclusive = true;
			}
		}

		// handle next flag
		if (next) {
			Timestamp nextDateTime = null;
			SelectConditionStep<Record1<Timestamp>> nex = dsl.select(min(cwmsTsInstView.DATE_TIME))
					.from(cwmsTsInstView)
					.where(whereCondition.and(dateTimeCol.greaterThan(Timestamp.from(endTime)))
							.and(startTimeCol.le(Timestamp.from(endTime))));
			nextDateTime = nex.fetchOne().value1();
			if (nextDateTime != null) {
				endTime = nextDateTime.toInstant();
				endInclusive = true;
			}
		}

		// Add the time windows conditions depending on the inclusive flags
        if (startInclusive && endInclusive){
			whereCondition = whereCondition
				.and(cwmsTsInstView.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
				.and(cwmsTsInstView.LAST_DATE_TIME.le(Timestamp.from(endTime)));
		} else if (!startInclusive && endInclusive) {
			whereCondition = whereCondition
				.and(cwmsTsInstView.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
				.and(cwmsTsInstView.LAST_DATE_TIME.le(Timestamp.from(endTime)));
		} else if (startInclusive) {
			whereCondition = whereCondition
				.and(cwmsTsInstView.FIRST_DATE_TIME.ge(Timestamp.from(startTime)))
				.and(cwmsTsInstView.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		} else {
			whereCondition = whereCondition
				.and(cwmsTsInstView.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
				.and(cwmsTsInstView.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		}
		Condition finalWhereCondition = whereCondition;

		// set semi-final variables for lambda
		final String recordCursor = cursor;
		final int recordPageSize = pageSize;

		// Get the total number of records if not already set
		if (total == null) {
			SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
					.from(cwmsTsInstView)
					.where(finalWhereCondition);
			total = count.fetchOne().value1();
		}

		// Get the max version date if needed
		Timestamp maxVersionDate = null;
		if (maxVersion) {
			SelectConditionStep<Record1<Timestamp>> maxVer = dsl.select(max(cwmsTsInstView.VERSION_DATE))
					.from(cwmsTsInstView)
					.where(finalWhereCondition);
			maxVersionDate = maxVer.fetchOne().value1();
		}
		Timestamp minVersionDate = null;
		if (!maxVersion && versionDate == null) {
			SelectConditionStep<Record1<Timestamp>> minVer = dsl.select(min(cwmsTsInstView.VERSION_DATE))
					.from(cwmsTsInstView)
					.where(finalWhereCondition);
			minVersionDate = minVer.fetchOne().value1();
		}

		// generate and run query to get the time series profile data
		Result<Record7<Double, Long, Timestamp, Long, Long, String, String>> result = null;
		SelectSeekStep1<Record7<Double, Long, Timestamp, Long, Long, String, String>, Timestamp> resultQuery = null;
		SelectConditionStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultCondQuery = null;
		SelectSeekLimitStep<Record7<Double, Long, Timestamp, Long, Long, String, String>> resultQuery2 = null;
		if (pageSize != 0) {
			if (maxVersion) {
				resultCondQuery = dsl.select(cwmsTsInstView.VALUE,
								cwmsTsInstView.QUALITY_CODE,
								cwmsTsInstView.DATE_TIME,
								cwmsTsInstView.LOCATION_CODE,
								cwmsTsInstView.KEY_PARAMETER_CODE,
								cwmsTsInstView.PARAMETER_ID,
								cwmsTsInstView.UNIT_ID)
						.from(cwmsTsInstView)
						.where(finalWhereCondition.and(cwmsTsInstView.VERSION_DATE.eq(maxVersionDate)));
			} else if (versionDate == null) {
				resultCondQuery = dsl.select(cwmsTsInstView.VALUE,
								cwmsTsInstView.QUALITY_CODE,
								cwmsTsInstView.DATE_TIME,
								cwmsTsInstView.LOCATION_CODE,
								cwmsTsInstView.KEY_PARAMETER_CODE,
								cwmsTsInstView.PARAMETER_ID,
								cwmsTsInstView.UNIT_ID)
						.from(cwmsTsInstView)
						.where(finalWhereCondition.and(cwmsTsInstView.VERSION_DATE.eq(minVersionDate)));
			} else {
				resultCondQuery = dsl.select(cwmsTsInstView.VALUE,
								cwmsTsInstView.QUALITY_CODE,
								cwmsTsInstView.DATE_TIME,
								cwmsTsInstView.LOCATION_CODE,
								cwmsTsInstView.KEY_PARAMETER_CODE,
								cwmsTsInstView.PARAMETER_ID,
								cwmsTsInstView.UNIT_ID)
						.from(cwmsTsInstView)
						.where(finalWhereCondition);
			}

			// If there is a cursor, use it with the JOOQ seek method
			// Needs the parameter and cursor of the record before the first one on the next page
			// 		to correctly split the data into pages
			// Searches for matching records based on the cursor to start the next page's results
			if (tsCursor == null) {
				resultQuery = resultCondQuery.and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(startTime.toEpochMilli()))))
						.and(dateTimeCol.lessOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
						.orderBy(cwmsTsInstView.DATE_TIME);

			} else {
				resultQuery2 = resultCondQuery
						.and(dateTimeCol.greaterOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(tsCursor.toInstant().toEpochMilli()))))
						.and(dateTimeCol.lessOrEqual(CWMS_UTIL_PACKAGE
								.call_TO_TIMESTAMP__2(val(endTime.toEpochMilli()))))
						.orderBy(cwmsTsInstView.DATE_TIME, cwmsTsInstView.PARAMETER_ID)
						.seek(tsCursor, parameterId);
			}

			// Get the results
			// if the page number is set, limit the results to the page size (plus one for setting the next page)
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

		// Throw 404 if no results
		if (result == null || result.isEmpty()) {
			throw new NotFoundException("No time series profile data found for the given parameters");
		}

		// map the results to a TimeSeriesProfileInstance
		Result<?> finalResult = result;
		int totalRecords = total;
		TS_PROF_DATA_T timeSeriesProfileData;
		BigInteger locationCode = null;
		BigInteger keyParameterCode = null;
		STR_TAB_T units = new STR_TAB_T(unit);
		TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();
		Map<Timestamp, Map<String, PVQ_T>> timeValuePairMap = new TreeMap<>();
		Map<String, String> unitParamMap = new TreeMap<>();

		// boolean to keep track of data that is consistent across all records
		boolean parentData = false;
		for (Record resultRecord : finalResult) {
			if (!parentData) {
				locationCode = resultRecord.get("LOCATION_CODE", BigInteger.class);
				keyParameterCode = resultRecord.get("KEY_PARAMETER_CODE", BigInteger.class);
				parentData = true;
			}

			// map the unit to the parameter
			if (unitParamMap.get(resultRecord.get("PARAMETER_ID", String.class)) == null) {
				unitParamMap.put(resultRecord.get("PARAMETER_ID", String.class),
						resultRecord.get("UNIT_ID", String.class));
			}

			// map the parameter, TVQ data
			Timestamp dateTime = resultRecord.get("DATE_TIME", Timestamp.class);
			Map<String, PVQ_T> dataMap;
			if (timeValuePairMap.get(dateTime) == null) {
				dataMap = new TreeMap<>();
			} else {
				dataMap = timeValuePairMap.get(dateTime);
			}
			dataMap.put(resultRecord.get("PARAMETER_ID", String.class), new PVQ_T(keyParameterCode,
					resultRecord.get("VALUE", Double.class),
					resultRecord.get("QUALITY_CODE", BigInteger.class)));
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

		timeSeriesProfileData = new TS_PROF_DATA_T(locationCode, keyParameterCode, timeZone, units, records);

		if (minVersionDate != null) {
			versionDate = minVersionDate.toInstant();
		} else if (maxVersionDate != null) {
			versionDate = maxVersionDate.toInstant();
		}

		// map the TimeSeriesProfileInstance without the value/quality data
		TimeSeriesProfileInstance returnInstance = map(location.getOfficeId(), location.getName(), keyParameter,
				timeSeriesProfileData, version, versionDate, startTime, endTime, unitParamMap, recordCursor,
				recordPageSize, totalRecords);

		List<ParameterColumnInfo> paramlist = returnInstance.getParameterColumns();

		// map the TVQ data to the TimeSeriesProfileInstance
		// needs previous parameter and cursor to be set to correctly split the data into pages
		// adds page, nextpage data to the TimeSeriesProfileInstance
		String previousParameter = null;
		Timestamp previousCursor = Timestamp.from(startTime);
		for (Map.Entry<Timestamp, Map<String, PVQ_T>> entry : timeValuePairMap.entrySet()) {
			for (Map.Entry<String, PVQ_T> dataValue : entry.getValue().entrySet()) {
				Timestamp dateTime = entry.getKey();
				returnInstance.addValue(dateTime, dataValue.getValue().getVALUE(), dataValue.getValue().getQUALITY_CODE().intValue(),
						previousParameter, previousCursor);
				previousCursor = dateTime;
				previousParameter = dataValue.getKey();
			}
		}

		// add null values to the TimeSeriesProfileInstance value list if the data is missing for the associated parameter
		for (Map.Entry<Long, List<TimeSeriesData>> entry : returnInstance.getTimeSeriesList().entrySet()) {
			if (entry.getValue().size() < paramlist.size()) {
				for (int i = 0; i < paramlist.size(); i++) {
					Timestamp dateTime = Timestamp.from(Instant.ofEpochMilli(entry.getKey()));
					try {
						entry.getValue().get(i);
					} catch (IndexOutOfBoundsException e) {
						returnInstance.addNullValue(dateTime, i);
						continue;
					}
					if (timeValuePairMap.get(dateTime) == null
							|| !timeValuePairMap.get(dateTime).containsKey(paramlist.get(i).getParameter())) {
						returnInstance.addNullValue(dateTime, i);
					}
				}
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