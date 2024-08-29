package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ProfileTimeSeries;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.data.dto.timeseriesprofile.TimeValuePair;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_INST;
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

			List<ProfileTimeSeries> timeSeriesList = timeseriesProfileInstance.getTimeSeriesList();
			for(ProfileTimeSeries profileTimeSeries : timeSeriesList)
			{
				parameter = profileTimeSeries.getParameter();
				parameterCodeDec = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_CODE(DSL.using(conn).configuration(), parameter,
						timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
				parameterIdToCode.put(parameter, parameterCodeDec.toBigInteger());
			}

			TS_PROF_DATA_T tsProfileData = new TS_PROF_DATA_T();
			tsProfileData.attach(DSL.using(conn).configuration());

			TS_PROF_DATA_TAB_T records = new TS_PROF_DATA_TAB_T();

			for(int i=0; i<timeseriesProfileInstance.getTimeSeriesList().get(0).getValues().size();i++)
			{
				TS_PROF_DATA_REC_T dataRecord = new TS_PROF_DATA_REC_T();
				Timestamp timeStamp =  Timestamp.from(timeseriesProfileInstance.getTimeSeriesList().get(0)
								.getValues().get(i).getDateTime());
				dataRecord.setDATE_TIME(timeStamp);
				records.add(dataRecord);
			}
			STR_TAB_T units = new STR_TAB_T();
			for (TS_PROF_DATA_REC_T ts_prof_data_rec_t : records) {
				PVQ_TAB_T parameters = new PVQ_TAB_T();
				for (int i = 0; i < timeseriesProfileInstance.getTimeSeriesList().size(); i++) {
					PVQ_T pvq = new PVQ_T();
					String parameterId = timeseriesProfileInstance.getTimeSeriesList().get(i).getParameter();
					BigInteger parameterCode = parameterIdToCode.get(parameterId);
					pvq.setPARAMETER_CODE(parameterCode);
					parameters.add(pvq);
					units.add(timeseriesProfileInstance.getTimeSeriesList().get(i).getUnit());
				}
				ts_prof_data_rec_t.setPARAMETERS(parameters);
			}

			for(int i = 0; i<timeseriesProfileInstance.getTimeSeriesList().size(); i++)
			{
				for(int j =0; j<timeseriesProfileInstance.getTimeSeriesList().get(i).getValues().size();j++)
				{
					TS_PROF_DATA_REC_T dataRecord = records.get(j);
					dataRecord.getPARAMETERS().get(i).setVALUE(
						timeseriesProfileInstance.getTimeSeriesList().get(i).getValues().get(j).getValue());
				}
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
		String maxVersion)
	{
		return connectionResult(dsl, conn -> {
			setOffice(conn, location.getOfficeId());
			TS_PROF_DATA_T timeSeriesProfileData;
				timeSeriesProfileData = CWMS_TS_PROFILE_PACKAGE.call_RETRIEVE_TS_PROFILE_DATA(
						DSL.using(conn).configuration(),
						location.getName(),
						keyParameter,
						version,
						unit,
						Timestamp.from(startTime),
						Timestamp.from(endTime),
						timeZone,
						startInclusive,
						endInclusive,
						previous,
						next,
						versionDate!=null?Timestamp.from(versionDate):null,
						maxVersion,
						location.getOfficeId()
				);

			return map(DSL.using(conn).configuration(), location.getOfficeId(), timeSeriesProfileData, version, versionDate);
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


	private TimeSeriesProfileInstance map(@NotNull Configuration configuration, String officeId, TS_PROF_DATA_T timeSeriesProfileData, String version, Instant versionDate)  {
		String timeZone = timeSeriesProfileData.getTIME_ZONE();
		STR_TAB_T units = timeSeriesProfileData.getUNITS();
		TS_PROF_DATA_TAB_T records = timeSeriesProfileData.getRECORDS();
		BigInteger locationCode = timeSeriesProfileData.getLOCATION_CODE();
		String location = CWMS_UTIL_PACKAGE.call_GET_LOCATION_ID(configuration, locationCode, officeId);
		BigInteger keyParameterCode = timeSeriesProfileData.getKEY_PARAMETER();
		String keyParameter = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_ID(configuration, keyParameterCode);
		List<Instant> timeList = new ArrayList<>();
		List<List<Double>> valuesList = new ArrayList<>();
		List<List<Integer>> qualitiesList = new ArrayList<>();
		List<List<BigInteger>> parametersList = new ArrayList<>();
		for(TS_PROF_DATA_REC_T dataRecord : records)
		{
			Instant dateTime = dataRecord.get(0, Instant.class);
			timeList.add(dateTime);
			PVQ_TAB_T parameters = dataRecord.getPARAMETERS();
			List<Double> valueList = new ArrayList<>();
			List<Integer> qualityList = new ArrayList<>();
			List<BigInteger> parameterList = new ArrayList<>();
			for(PVQ_T parameter : parameters)
			{
				valueList.add(parameter.getVALUE());
				qualityList.add(parameter.getQUALITY_CODE().intValue());
				parameterList.add(parameter.getPARAMETER_CODE());
			}
			valuesList.add(valueList);
			parametersList.add(parameterList);
			qualitiesList.add(qualityList);
		}
		List<String> parameterList = new ArrayList<>();
		List<List<TimeValuePair>> timeValuePairList = new ArrayList<>();
		if(!parametersList.isEmpty()) {
			for (int i = 0; i < parametersList.get(0).size(); i++) {
				String parameter = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_ID(configuration, parametersList.get(0).get(i));
				parameterList.add(parameter);
				timeValuePairList.add(new ArrayList<>());
			}
		}
		if(!valuesList.isEmpty())
		{
			for(int i = 0; i<valuesList.size(); i++) {
				for (int j = 0; j < valuesList.get(i).size(); j++) {
					TimeValuePair timeValuePair = new TimeValuePair.Builder()
							.withDateTime(timeList.get(i))
							.withValue(valuesList.get(i).get(j))
							.withQuality(qualitiesList.get(i).get(j))
							.build();
					timeValuePairList.get(j).add(timeValuePair);
				}
			}}
		List<ProfileTimeSeries> timeSeriesList = new ArrayList<>();
		if(!timeValuePairList.isEmpty()) {
			for (int i = 0; i < timeValuePairList.size(); i++) {
				ProfileTimeSeries timeSeries = new ProfileTimeSeries.Builder()
						.withValues(timeValuePairList.get(i))
						.withTimeZone(timeZone)
						.withParameter(parameterList.get(i))
						.withUnit(units.get(i))
						.build();
				timeSeriesList.add(timeSeries);
			}
		}
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
				.withFirstDate(timeList.stream().min(Instant::compareTo).orElse(null))
				.withLastDate(timeList.stream().max(Instant::compareTo).orElse(null))
				.withVersionDate(versionDate)
				.build();
	}
}