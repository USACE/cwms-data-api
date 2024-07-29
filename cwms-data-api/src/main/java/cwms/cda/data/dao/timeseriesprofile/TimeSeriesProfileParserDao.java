package cwms.cda.data.dao.timeseriesprofile;

import java.util.ArrayList;
import java.util.List;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_ts_profile.RETRIEVE_TS_PROFILE_PARSER;


public class TimeSeriesProfileParserDao extends JooqDao<TimeSeriesProfileParser>
{
	public TimeSeriesProfileParserDao(DSLContext dsl)
	{
		super(dsl);
	}

	private List<ParameterInfo> getParameterInfoList(String info, String recordDelimiter, String fieldDelimiter)
	{
		List<ParameterInfo> parameterInfoList = new ArrayList<>();
		String[] records = info.split(recordDelimiter);
		for(String aRecord : records)
		{
			String[] fields = aRecord.split(fieldDelimiter);
			int index = Integer.parseInt(fields[2]);
			ParameterInfo parameterInfo = new ParameterInfo.Builder().withParameter(fields[0])
																	 .withUnit(fields[1])
																	 .withIndex(index)
																	 .build();
			parameterInfoList.add(parameterInfo);
		}
		return parameterInfoList;
	}

	private String getParameterInfoString(TimeSeriesProfileParser timeSeriesProfileParser)
	{
		List<ParameterInfo> parameterInfo = timeSeriesProfileParser.getParameterInfoList();

		StringBuilder parameterInfoBuilder = new StringBuilder();
		parameterInfoBuilder.append(parameterInfo.get(0).getParameter())
				.append(",")
				.append(parameterInfo.get(0).getUnit())
				.append(",")
				.append( parameterInfo.get(0).getIndex()!=null?parameterInfo.get(0).getIndex():"")
				.append(",")
				.append( parameterInfo.get(0).getStartColumn()!=null?parameterInfo.get(0).getStartColumn():"")
				.append(",")
				.append( parameterInfo.get(0).getEndColumn()!=null?parameterInfo.get(0).getEndColumn():"");
		for(int i = 1; i < parameterInfo.size(); i++)
		{
			parameterInfoBuilder.append(timeSeriesProfileParser.getRecordDelimiter())
					.append(parameterInfo.get(i).getParameter())
					.append(",")
					.append(parameterInfo.get(i).getUnit())
					.append(",")
					.append( parameterInfo.get(i).getIndex()!=null?parameterInfo.get(i).getIndex():"")
					.append(",")
					.append( parameterInfo.get(i).getStartColumn()!=null?parameterInfo.get(i).getStartColumn():"")
					.append(",")
					.append( parameterInfo.get(i).getEndColumn()!=null?parameterInfo.get(i).getEndColumn():"");
		}
		return parameterInfoBuilder.toString();
	}

	public void storeTimeSeriesProfileParser(TimeSeriesProfileParser timeSeriesProfileParser, boolean failIfExists)
	{
		connection(dsl, conn ->
			CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_PARSER(DSL.using(conn).configuration(), timeSeriesProfileParser.getLocationId().getName(),
					timeSeriesProfileParser.getKeyParameter(), String.valueOf(timeSeriesProfileParser.getRecordDelimiter()),
					timeSeriesProfileParser.getFieldDelimiter()!=null ? String.valueOf(timeSeriesProfileParser.getFieldDelimiter()) : null, timeSeriesProfileParser.getTimeField(),
					timeSeriesProfileParser.getTimeStartColumn(), timeSeriesProfileParser.getTimeEndColumn(), timeSeriesProfileParser.getTimeFormat(),
					timeSeriesProfileParser.getTimeZone(), getParameterInfoString(timeSeriesProfileParser),
					timeSeriesProfileParser.getTimeInTwoFields()?"T":"F",
					failIfExists?"T":"F", timeSeriesProfileParser.getFieldDelimiter()!=null?"T":"F", timeSeriesProfileParser.getLocationId().getOfficeId())
		);
	}

	public TimeSeriesProfileParser retrieveTimeSeriesProfileParser(String locationId, String parameterId, String officeId)
	{
		return connectionResult(dsl, conn -> {
			RETRIEVE_TS_PROFILE_PARSER timeSeriesProfileParser = CWMS_TS_PROFILE_PACKAGE.call_RETRIEVE_TS_PROFILE_PARSER(
					DSL.using(conn).configuration(), locationId, parameterId, officeId);
			return map(timeSeriesProfileParser, locationId,  parameterId, officeId);
		});
	}

	public List<TimeSeriesProfileParser> retrieveTimeSeriesProfileParsers(String locationIdMask, String parameterIdMask, String officeIdMask)
	{
		return connectionResult(dsl, conn -> {
			Result<Record> tsProfileParserResult = CWMS_TS_PROFILE_PACKAGE.call_CAT_TS_PROFILE_PARSER(DSL.using(conn).configuration(),
					locationIdMask, parameterIdMask, officeIdMask);
			List<TimeSeriesProfileParser> timeSeriesProfileParserList = new ArrayList<>();
			for(Record profileParser : tsProfileParserResult)
			{
				String recordDelimiter = profileParser.get("RECORD_DELIMITER", String.class);
				String fieldDelimiter = profileParser.get("FIELD_DELIMITER", String.class);
				Short timeField = profileParser.get("TIME_FIELD", Short.class);

				Result<Record> parameterInfoResult = profileParser.get("PARAMETER_INFO", Result.class);

				List<ParameterInfo> parameterInfoList = new ArrayList<>();
					for(Record record : parameterInfoResult)
					{
						parameterInfoList.add(new ParameterInfo.Builder()
								.withParameter((String) record.get("PARAMETER_ID"))
								.withUnit((String) record.get("UNIT"))
								.withIndex( record.get("FIELD_NUMBER", Short.class))
								.build());
					}


				CwmsId locationId = new CwmsId.Builder()
						.withOfficeId((String) profileParser.get("OFFICE_ID"))
						.withName((String) profileParser.get("LOCATION_ID"))
						.build();
				TimeSeriesProfileParser timeSeriesProfileParser = new TimeSeriesProfileParser.Builder()
						.withLocationId(locationId)
						.withKeyParameter((String) profileParser.get("KEY_PARAMTER_ID"))
						.withTimeFormat((String) profileParser.get("TIME_FORMAT"))
						.withTimeZone((String) profileParser.get("TIME_ZONE"))
						.withRecordDelimiter(recordDelimiter.toCharArray()[0])
						.withFieldDelimiter(fieldDelimiter.toCharArray()[0])
						.withTimeField(timeField)
						.withParameterInfoList(parameterInfoList)
						.build();
				timeSeriesProfileParserList.add(timeSeriesProfileParser);
			}
			return timeSeriesProfileParserList;
		});
	}


	public void copyTimeSeriesProfileParser(String locationId, String parameterId, String officeId, String destinationLocation)
	{
		connection(dsl, conn ->
				CWMS_TS_PROFILE_PACKAGE.call_COPY_TS_PROFILE_PARSER(DSL.using(conn).configuration(),locationId, parameterId, destinationLocation,
							 "F", officeId));
	}
	public void deleteTimeSeriesProfileParser(String locationId, String parameterId, String officeId)
	{
		connection(dsl, conn ->
			CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE_PARSER(DSL.using(conn).configuration(), locationId,
					parameterId, officeId)
		);
	}
	private TimeSeriesProfileParser map(RETRIEVE_TS_PROFILE_PARSER timeSeriesProfileParser, String locationName, String keyParameter, String officeId)
	{
		String info = timeSeriesProfileParser.getP_PARAMETER_INFO();
		List<ParameterInfo> parameterInfo = getParameterInfoList(info, timeSeriesProfileParser.getP_RECORD_DELIMITER(),
				timeSeriesProfileParser.getP_FIELD_DELIMITER());
		CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(locationName).build();
		return new TimeSeriesProfileParser.Builder()
				.withLocationId(locationId)
				.withTimeField(timeSeriesProfileParser.getP_TIME_FIELD())
				.withTimeZone(timeSeriesProfileParser.getP_TIME_ZONE())
				.withTimeFormat(timeSeriesProfileParser.getP_TIME_FORMAT())
				.withKeyParameter(keyParameter)
				.withFieldDelimiter(timeSeriesProfileParser.getP_FIELD_DELIMITER().toCharArray()[0])
				.withRecordDelimiter(timeSeriesProfileParser.getP_RECORD_DELIMITER().toCharArray()[0])
				.withTimeInTwoFields(false)
				.withParameterInfoList(parameterInfo)
				.build();
	}
}
