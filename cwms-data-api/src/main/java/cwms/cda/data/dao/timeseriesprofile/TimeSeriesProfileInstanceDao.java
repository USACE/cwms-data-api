package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.DataColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterColumnInfo;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesData;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class TimeSeriesProfileInstanceDao extends JooqDao<TimeSeriesProfileInstance>
{
	public TimeSeriesProfileInstanceDao(DSLContext dsl)
	{
		super(dsl);
	}

	public void storeTimeSeriesProfileInstance(TimeSeriesProfile timeSeriesProfile, String profileData, Instant versionDate,
			String versionId, String storeRule, boolean overrideProtection)
	{
		connection(dsl, conn -> {
			setOffice(conn, timeSeriesProfile.getLocationId().getOfficeId());
			CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE__2(DSL.using(conn).configuration(),
					timeSeriesProfile.getLocationId().getName(),
					timeSeriesProfile.getKeyParameter(),
					profileData,
					versionId,
					storeRule,
					overrideProtection?"T":"F",
					versionDate!=null?Timestamp.from(versionDate):null,
					timeSeriesProfile.getLocationId().getOfficeId());
		});
	}

	public void storeTimeSeriesProfileInstance(TimeSeriesProfileInstance timeseriesProfileInstance, String versionId, Instant versionInstant, String storeRule,String overrideProtection)
	{
		connection(dsl, conn -> {
			setOffice(conn, timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
			BigDecimal locationCodeId = CWMS_LOC_PACKAGE.call_GET_LOCATION_CODE(DSL.using(conn).configuration(),
					timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId(),
					timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getName());


			Map<String, BigInteger> parameterIdToCode = new HashMap<>();

			String parameter = timeseriesProfileInstance.getTimeSeriesProfile().getKeyParameter();
			BigDecimal parameterCodeDec = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_CODE(DSL.using(conn).configuration(), parameter,
					timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
			parameterIdToCode.put(parameter, parameterCodeDec.toBigInteger());

			List<String> dependentParameters = timeseriesProfileInstance.getTimeSeriesProfile().getParameterList();
			for(String param : dependentParameters)
			{
				parameter = param;
				parameterCodeDec = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_CODE(DSL.using(conn).configuration(), parameter,
						timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
				parameterIdToCode.put(parameter, parameterCodeDec.toBigInteger());
			}

			TS_PROF_DATA_T tsProfileData = new TS_PROF_DATA_T();
			tsProfileData.attach(DSL.using(conn).configuration());

			TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();

			for(Long timestamp : timeseriesProfileInstance.getTimeSeriesList().keySet())
			{
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
			for(Long timestamp : timeseriesProfileInstance.getTimeSeriesList().keySet())
			{
				for(int j =0; j<timeseriesProfileInstance.getTimeSeriesList().get(timestamp).size();j++)
				{
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



				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE(DSL.using(conn).configuration(),
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

		@NotNull Result<Record> timeSeriesProfileInstanceResults =  dsl.select(DSL.asterisk()).from(AV_TS_PROFILE_INST.AV_TS_PROFILE_INST)
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
		String unit,
		Instant startTime,
		Instant endTime,
		String timeZone,
		String startInclusive,
		String endInclusive,
		String previous,
		String next,
		Instant versionDate,
		String maxVersion,
		int page,
		int pageSize,
		boolean getTotal)
	{
		Condition whereCondition = AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.KEY_PARAMETER_ID.eq(keyParameter)
				.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.LOCATION_ID.eq(location.getName())
				.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.OFFICE_ID.eq(location.getOfficeId()))
				.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.VERSION_ID.eq(version))
				.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.VERSION_DATE.eq(Timestamp.from(versionDate)))
				.and((AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.UNIT_ID.eq(unit.split(",")[0]))
					.or(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.UNIT_ID.eq(unit.split(",")[1]))));

		if(OracleTypeMap.parseBool(startInclusive) && OracleTypeMap.parseBool(endInclusive)){
			whereCondition = whereCondition
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.LAST_DATE_TIME.le(Timestamp.from(endTime)))
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.FIRST_DATE_TIME.ge(Timestamp.from(startTime)));
		} else if (!OracleTypeMap.parseBool(startInclusive) && (OracleTypeMap.parseBool(endInclusive))) {
			whereCondition = whereCondition
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.LAST_DATE_TIME.eq(Timestamp.from(endTime)))
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)));
		} else if (OracleTypeMap.parseBool(startInclusive) && !OracleTypeMap.parseBool(endInclusive)) {
			whereCondition = whereCondition
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.FIRST_DATE_TIME.eq(Timestamp.from(startTime)))
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		} else {
			whereCondition = whereCondition
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.FIRST_DATE_TIME.greaterThan(Timestamp.from(startTime)))
							.and(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.LAST_DATE_TIME.lessThan(Timestamp.from(endTime)));
		}
		Condition finalWhereCondition = whereCondition;

		return connectionResult(dsl, conn -> {
			setOffice(conn, location.getOfficeId());
			TS_PROF_DATA_T timeSeriesProfileData;
			if (getTotal) {
				Result<Record> result = dsl.select(DSL.asterisk())
					.from(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2)
					.where(finalWhereCondition)
					.fetch();

				return new TimeSeriesProfileInstance.Builder()
						.withTotal(result.size())
						.build();
			}
			Result<Record> result = dsl.select(DSL.asterisk())
				.from(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2)
				.where(finalWhereCondition)
				.orderBy(AV_TS_PROFILE_INST_TSV2.AV_TS_PROFILE_INST_TSV2.KEY_PARAMETER_ID)
				.limit(pageSize)
				.offset(pageSize * (page - 1))
				.fetch();
			if (result.isEmpty()) {
				throw new NotFoundException("No time series profile data found for the given parameters");
			}

			BigInteger locationCode = null;
			BigInteger keyParameterCode = null;
			STR_TAB_T units = new STR_TAB_T(unit.split(","));
			TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();
			Map<Timestamp, PVQ_TAB_T> timeValuePairMap = new TreeMap<>();
			Map<String, String> unitParamMap = new TreeMap<>();

			boolean parentData = false;
			for (Record resultRecord : result) {
				if (!parentData)
				{
					locationCode = resultRecord.get("LOCATION_CODE", BigInteger.class);
					keyParameterCode = resultRecord.get("KEY_PARAMETER_CODE", BigInteger.class);
					parentData = true;
				}
				if (unitParamMap.get(resultRecord.get("PARAMETER_ID", String.class)) == null)
				{
					unitParamMap.put(resultRecord.get("PARAMETER_ID", String.class), resultRecord.get("UNIT_ID", String.class));
				}
				Timestamp dateTime = resultRecord.get("DATE_TIME", Timestamp.class);

				if (timeValuePairMap.get(dateTime) != null) {
					PVQ_TAB_T existingParameters = timeValuePairMap.get(dateTime);
					existingParameters.add(new PVQ_T(keyParameterCode, resultRecord.get("VALUE", Double.class),
							resultRecord.get("QUALITY_CODE", BigInteger.class)));
					timeValuePairMap.put(dateTime, existingParameters);
				} else {
					PVQ_TAB_T parameters = new PVQ_TAB_T(new PVQ_T(keyParameterCode, resultRecord.get("VALUE", Double.class),
						resultRecord.get("QUALITY_CODE", BigInteger.class)));
					timeValuePairMap.put(dateTime, parameters);
				}
			}
			int index = 0;
			for (Map.Entry<Timestamp, PVQ_TAB_T> entry : timeValuePairMap.entrySet()) {
				records.add(index, new TS_PROF_DATA_REC_T(entry.getKey(), entry.getValue()));
				index++;
			}
			timeSeriesProfileData = new TS_PROF_DATA_T(locationCode, keyParameterCode, timeZone, units, records);

			return map(DSL.using(conn).configuration(), location.getOfficeId(), timeSeriesProfileData, version,
					versionDate, startTime, endTime, unitParamMap, page, pageSize);
		});
	}
	public void deleteTimeSeriesProfileInstance(CwmsId location, String keyParameter,
			String version, Instant firstDate, String timeZone,boolean overrideProtection, Instant versionDate)
	{
		connection(dsl, conn -> {
			setOffice(conn, location.getOfficeId());
			Timestamp versionTimestamp = null;
			if(versionDate!=null)
			{
				versionTimestamp = Timestamp.from(versionDate);
			}

				CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE_INSTANCE(
						DSL.using(conn).configuration(),
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


	private TimeSeriesProfileInstance map(@NotNull Configuration configuration, String officeId,
			TS_PROF_DATA_T timeSeriesProfileData, String version, Instant versionDate, Instant startTime,
			Instant endTime, Map<String, String> unitParamMap, int page, int pageSize)  {
		String timeZone = timeSeriesProfileData.getTIME_ZONE();
//		STR_TAB_T units = timeSeriesProfileData.getUNITS();
		TS_PROF_DATA_TAB_T records = timeSeriesProfileData.getRECORDS();
		BigInteger locationCode = timeSeriesProfileData.getLOCATION_CODE();
		String location = CWMS_UTIL_PACKAGE.call_GET_LOCATION_ID(configuration, locationCode, officeId);
		BigInteger keyParameterCode = timeSeriesProfileData.getKEY_PARAMETER();
		String keyParameter = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_ID(configuration, keyParameterCode);
		List<Instant> timeList = new ArrayList<>();
		List<DataColumnInfo> dataColumnInfoList = new ArrayList<>();
		DataColumnInfo valueDataColumnInfo = new DataColumnInfo.Builder()
				.withName("value")
				.withOrdinal(1)
				.withDataType("java.lang.Double")
				.build();
		DataColumnInfo qualityDataColumnInfo = new DataColumnInfo.Builder()
				.withName("quality")
				.withOrdinal(2)
				.withDataType("java.lang.Integer")
				.build();
		dataColumnInfoList.add(valueDataColumnInfo);
		dataColumnInfoList.add(qualityDataColumnInfo);
        Map<Long, List<TimeSeriesData>> timeSeriesList = new TreeMap<>();

		for(TS_PROF_DATA_REC_T dataRecord : records)
		{
			List<TimeSeriesData> timeSeriesDataList = new ArrayList<>();
			Instant dateTime = dataRecord.get(0, Instant.class);
			timeList.add(dateTime);
			PVQ_TAB_T parameters = dataRecord.getPARAMETERS();
			for(PVQ_T parameter : parameters)
			{
				TimeSeriesData qualityValuePair
						= new TimeSeriesData(parameter.getVALUE(), parameter.getQUALITY_CODE().intValue());
				timeSeriesDataList.add(qualityValuePair);
			}


			timeSeriesList.put(dateTime.toEpochMilli(), timeSeriesDataList);
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
				.withTimeSeriesList(timeSeriesList)
				.withVersion(version)
				.withFirstDate(startTime)
				.withLastDate(endTime)
				.withLocationTimeZone(timeZone)
				.withPage(page)
				.withPageSize(pageSize)
//				.withCursor(cursor)
				.withDataColumns(dataColumnInfoList)
				.withParameterColumns(parameterColumnInfoList)
				.withPageFirstDate(timeList.stream().min(Instant::compareTo).orElse(null))
				.withPageLastDate(timeList.stream().max(Instant::compareTo).orElse(null))
				.withVersionDate(versionDate)
				.build();
	}
}