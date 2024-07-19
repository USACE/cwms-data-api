package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ProfileTimeSeries;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.data.dto.timeseriesprofile.TimeValuePair;
import org.jetbrains.annotations.NotNull;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
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

	void storeTimeSeriesProfileInstance(TimeSeriesProfile timeSeriesProfile, String profileData, Instant versionDate,
			String versionId, String storeRule, boolean overrideProtection)
	{
		connection(dsl, conn -> {
			try {
				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE__2(DSL.using(conn).configuration(),
						timeSeriesProfile.getLocationId().getName(),
						timeSeriesProfile.getKeyParameter(),
						profileData,
						versionId,
						storeRule,
						overrideProtection?"T":"F",
						Timestamp.from(versionDate),
						timeSeriesProfile.getLocationId().getOfficeId());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});
	}

	void storeTimeSeriesProfileInstance(TimeSeriesProfileInstance timeseriesProfileInstance, String versionId, Instant versionInstant, String storeRule,String overrideProtection)
	{
		connection(dsl, conn -> {
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

			for(int i=0; i<timeseriesProfileInstance.getTimeSeriesList().get(0).getTimeValuePairList().size();i++)
			{
				TS_PROF_DATA_REC_T dataRecord = new TS_PROF_DATA_REC_T();
				Timestamp timeStamp =  Timestamp.from(timeseriesProfileInstance.getTimeSeriesList().get(0)
								.getTimeValuePairList().get(i).getDateTime());
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
					pvq.setQUALITY_CODE(BigInteger.valueOf(9));
					parameters.add(pvq);
					units.add(timeseriesProfileInstance.getTimeSeriesList().get(i).getUnit());
				}
				ts_prof_data_rec_t.setPARAMETERS(parameters);
			}

			for(int i = 0; i<timeseriesProfileInstance.getTimeSeriesList().size(); i++)
			{
				for(int j =0; j<timeseriesProfileInstance.getTimeSeriesList().get(i).getTimeValuePairList().size();j++)
				{
					TS_PROF_DATA_REC_T dataRecord = records.get(j);
					dataRecord.getPARAMETERS().get(i).setVALUE(
						timeseriesProfileInstance.getTimeSeriesList().get(i).getTimeValuePairList().get(j).getValue());
				}
			}

			tsProfileData.setRECORDS(records);
			tsProfileData.setLOCATION_CODE(locationCodeId.toBigInteger());
			tsProfileData.setTIME_ZONE("UTC");
			tsProfileData.setKEY_PARAMETER(parameterIdToCode.get(timeseriesProfileInstance.getTimeSeriesProfile().getKeyParameter()));
			tsProfileData.setUNITS(units);

			Timestamp versionTimeStamp =  Timestamp.from(versionInstant);

			try {


				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_INSTANCE(DSL.using(conn).configuration(),
						tsProfileData,
						versionId,
						storeRule,
						overrideProtection,
						versionTimeStamp,
						timeseriesProfileInstance.getTimeSeriesProfile().getLocationId().getOfficeId());
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			}
			);
	}
	List<TimeSeriesProfileInstance> retrieveTimeSeriesProfileInstances( String officeIdMask, String locationMask, String parameterMask, String versionMask,
			Instant startTime, Instant endTime, String timeZone)
	{
		return connectionResult(dsl, conn -> {
			List<TimeSeriesProfileInstance> instanceList = new ArrayList<>();
			Result<Record> results = CWMS_TS_PROFILE_PACKAGE.call_CAT_TS_PROFILE_INSTANCE(DSL.using(conn).configuration(),
					locationMask, parameterMask, versionMask, Timestamp.from(startTime), Timestamp.from(endTime),
					timeZone, officeIdMask);
			for (Record result : results) {
				CwmsId locationId = new CwmsId.Builder()
						.withOfficeId((String) result.get(0))
						.withName((String) result.get(1))
						.build();
				String parameterId = (String) result.get(2);
				TimeSeriesProfile timeSeriesProfile = new TimeSeriesProfile.Builder()
						.withLocationId(locationId)
						.withKeyParameter(parameterId)
						.build();
				TimeSeriesProfileInstance timeSeriesProfileInstance = new TimeSeriesProfileInstance.Builder()
						.withTimeSeriesProfile(timeSeriesProfile)
						.withVersion(result.get(3, String.class))
						.withVersionDate(result.get(4, Instant.class))
						.withFirstDate(result.get(5, Instant.class))
						.withLastDate(result.get(6, Instant.class))
						.build();

				instanceList.add(timeSeriesProfileInstance);
			}
			return instanceList;
		});
	}

	TimeSeriesProfileInstance retrieveTimeSeriesProfileInstance(CwmsId location, String keyParameter,
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
			TS_PROF_DATA_T timeSeriesProfileData;
			try {
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
						null,//Timestamp.from(versionDate),
						maxVersion,
						location.getOfficeId()
				);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				return null;
			}
			return map(DSL.using(conn).configuration(), location.getOfficeId(), timeSeriesProfileData);
		});
	}
	void deleteTimeSeriesProfileInstance(CwmsId location, String keyParameter,
			String version, Instant firstDate, String timeZone,boolean overrideProtection, Instant versionDate)
	{
		connection(dsl, conn -> {

			Timestamp versionTimestamp = null;
			if(versionDate!=null)
			{
				versionTimestamp = Timestamp.from(versionDate);
			}

			try {
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
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
			}
		});
	}


	private TimeSeriesProfileInstance map(@NotNull Configuration configuration, String officeId, TS_PROF_DATA_T timeSeriesProfileData)  {
		String timeZone = timeSeriesProfileData.getTIME_ZONE();
		STR_TAB_T units = timeSeriesProfileData.getUNITS();
		TS_PROF_DATA_TAB_T records = timeSeriesProfileData.getRECORDS();
		BigInteger locationCode = timeSeriesProfileData.getLOCATION_CODE();
		String location = CWMS_UTIL_PACKAGE.call_GET_LOCATION_ID(configuration, locationCode, officeId);
		BigInteger keyParameterCode = timeSeriesProfileData.getKEY_PARAMETER();
		String keyParameter = CWMS_UTIL_PACKAGE.call_GET_PARAMETER_ID(configuration, keyParameterCode);
		List<Instant> timeList = new ArrayList<>();
		List<List<Double>> valuesList = new ArrayList<>();
		List<List<BigInteger>> parametersList = new ArrayList<>();
		List<List<BigInteger>> qualitiesList = new ArrayList<>();
		for(TS_PROF_DATA_REC_T dataRecord : records)
		{
			Instant dateTime = dataRecord.get(0, Instant.class);
			timeList.add(dateTime);
			PVQ_TAB_T parameters = dataRecord.getPARAMETERS();
			List<Double> valueList = new ArrayList<>();
			List<BigInteger> qualityList = new ArrayList<>();
			List<BigInteger> parameterList = new ArrayList<>();
			for(PVQ_T parameter : parameters)
			{
				qualityList.add(parameter.getQUALITY_CODE());
				valueList.add(parameter.getVALUE());
				parameterList.add(parameter.getPARAMETER_CODE());
			}
			qualitiesList.add(qualityList);
			valuesList.add(valueList);
			parametersList.add(parameterList);
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
							.build();
					timeValuePairList.get(j).add(timeValuePair);
				}
			}}
		List<ProfileTimeSeries> timeSeriesList = new ArrayList<>();
		if(!timeValuePairList.isEmpty()) {
			for (int i = 0; i < timeValuePairList.size(); i++) {
				ProfileTimeSeries timeSeries = new ProfileTimeSeries.Builder()
						.withTimeValuePairList(timeValuePairList.get(i))
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
				.build();
	}
}