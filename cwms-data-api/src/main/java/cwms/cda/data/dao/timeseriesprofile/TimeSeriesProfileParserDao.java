package cwms.cda.data.dao.timeseriesprofile;

import java.util.ArrayList;
import java.util.List;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoColumnar;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
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
			ParameterInfo parameterInfo = new ParameterInfoIndexed.Builder().withIndex(index)
																	 .withParameter(fields[0])
																	 .withUnit(fields[1])
																	 .build();
			parameterInfoList.add(parameterInfo);
		}
		return parameterInfoList;
	}

	private String getParameterInfoString(TimeSeriesProfileParser timeSeriesProfileParser)
	{
		List<ParameterInfo> parameterInfo = timeSeriesProfileParser.getParameterInfoList();

		StringBuilder parameterInfoBuilder = new StringBuilder();
		parameterInfoBuilder.append(parameterInfo.get(0).parameterInfoString());

		for(int i = 1; i < parameterInfo.size(); i++)
		{
			parameterInfoBuilder.append(timeSeriesProfileParser.getRecordDelimiter())
					.append(parameterInfo.get(i).parameterInfoString());
		}
		return parameterInfoBuilder.toString();
	}

	public void storeTimeSeriesProfileParser(TimeSeriesProfileParserIndexed timeSeriesProfileParser, boolean failIfExists)
	{
		connection(dsl, conn ->
				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_PARSER(DSL.using(conn).configuration(), timeSeriesProfileParser.getLocationId().getName(),
						timeSeriesProfileParser.getKeyParameter(), String.valueOf(timeSeriesProfileParser.getRecordDelimiter()),
						String.valueOf(timeSeriesProfileParser.getFieldDelimiter()), timeSeriesProfileParser.getTimeField(),
						null, null, timeSeriesProfileParser.getTimeFormat(),
						timeSeriesProfileParser.getTimeZone(), getParameterInfoString(timeSeriesProfileParser),
						timeSeriesProfileParser.getTimeInTwoFields()?"T":"F",
						failIfExists?"T":"F", "T", timeSeriesProfileParser.getLocationId().getOfficeId())
		);
	}
	public void storeTimeSeriesProfileParser(TimeSeriesProfileParserColumnar timeSeriesProfileParser, boolean failIfExists)
	{
		connection(dsl, conn ->
				CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_PARSER(DSL.using(conn).configuration(), timeSeriesProfileParser.getLocationId().getName(),
						timeSeriesProfileParser.getKeyParameter(), String.valueOf(timeSeriesProfileParser.getRecordDelimiter()),
						 null, null,
						timeSeriesProfileParser.getTimeStartColumn(), timeSeriesProfileParser.getTimeEndColumn(), timeSeriesProfileParser.getTimeFormat(),
						timeSeriesProfileParser.getTimeZone(), getParameterInfoString(timeSeriesProfileParser),
						timeSeriesProfileParser.getTimeInTwoFields()?"T":"F",
						failIfExists?"T":"F", "F", timeSeriesProfileParser.getLocationId().getOfficeId())
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

	public List<TimeSeriesProfileParser> catalogTimeSeriesProfileParsers(String locationIdMask, String parameterIdMask, String officeIdMask)
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
				Short timeStartCol = profileParser.get("TIME_START_COL", Short.class);
				Short timeEndCol = profileParser.get("TIME_END_COL", Short.class);
				Result<Record> parameterInfoResult = profileParser.get("PARAMETER_INFO", Result.class);

				List<ParameterInfo> parameterInfoList = new ArrayList<>();
					for(Record recordParam : parameterInfoResult)
					{
						if(timeField!=null) {
							parameterInfoList.add(new ParameterInfoIndexed.Builder()
									.withIndex(recordParam.get("FIELD_NUMBER", Short.class))
									.withParameter((String) recordParam.get("PARAMETER_ID"))
									.withUnit((String) recordParam.get("UNIT"))
									.build());
						}
						else {
							parameterInfoList.add(new ParameterInfoColumnar.Builder()
									.withStartColumn(recordParam.get("START_COL", Short.class))
									.withEndColumn(recordParam.get("END_COL", Short.class))
									.withParameter((String) recordParam.get("PARAMETER_ID"))
									.withUnit((String) recordParam.get("UNIT"))
									.build());
						}
					}


				CwmsId locationId = new CwmsId.Builder()
						.withOfficeId((String) profileParser.get("OFFICE_ID"))
						.withName((String) profileParser.get("LOCATION_ID"))
						.build();
				TimeSeriesProfileParser timeSeriesProfileParser ;
				if(timeField!=null)
					{
						 timeSeriesProfileParser = new TimeSeriesProfileParserIndexed.Builder()
						.withFieldDelimiter(fieldDelimiter.toCharArray()[0])
						.withTimeField(timeField)
						.withLocationId(locationId)
						.withKeyParameter((String) profileParser.get("KEY_PARAMTER_ID"))
						.withTimeFormat((String) profileParser.get("TIME_FORMAT"))
						.withTimeZone((String) profileParser.get("TIME_ZONE"))
						.withRecordDelimiter(recordDelimiter.toCharArray()[0])
						.withParameterInfoList(parameterInfoList)
						.build();
						timeSeriesProfileParserList.add(timeSeriesProfileParser);
					}
				else if(timeStartCol!=null && timeEndCol!=null)
				{
					timeSeriesProfileParser = new TimeSeriesProfileParserColumnar.Builder()
							.withTimeStartColumn(timeStartCol)
							.withTimeEndColumn(timeEndCol)
							.withLocationId(locationId)
							.withKeyParameter((String) profileParser.get("KEY_PARAMTER_ID"))
							.withTimeFormat((String) profileParser.get("TIME_FORMAT"))
							.withTimeZone((String) profileParser.get("TIME_ZONE"))
							.withRecordDelimiter(recordDelimiter.toCharArray()[0])
							.withParameterInfoList(parameterInfoList)
							.build();
					timeSeriesProfileParserList.add(timeSeriesProfileParser);
				}

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
//				.withTimeField(timeSeriesProfileParser.getP_TIME_FIELD())
				.withTimeZone(timeSeriesProfileParser.getP_TIME_ZONE())
				.withTimeFormat(timeSeriesProfileParser.getP_TIME_FORMAT())
				.withKeyParameter(keyParameter)
//				.withFieldDelimiter(timeSeriesProfileParser.getP_FIELD_DELIMITER().toCharArray()[0])
				.withRecordDelimiter(timeSeriesProfileParser.getP_RECORD_DELIMITER().toCharArray()[0])
				.withTimeInTwoFields(false)
				.withParameterInfoList(parameterInfo)
				.build();
	}
}
