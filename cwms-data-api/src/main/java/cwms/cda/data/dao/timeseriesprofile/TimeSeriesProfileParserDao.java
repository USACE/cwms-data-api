package cwms.cda.data.dao.timeseriesprofile;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfo;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoColumnar;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_ts_profile.RETRIEVE_TS_PROFILE_PARSER;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_PARSER;
import usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE_PARSER_PARAM;


public class TimeSeriesProfileParserDao extends JooqDao<TimeSeriesProfileParser> {
    private static final AV_TS_PROFILE_PARSER_PARAM cwmsTsProfileParserParamView = AV_TS_PROFILE_PARSER_PARAM
            .AV_TS_PROFILE_PARSER_PARAM;
    private static final AV_TS_PROFILE_PARSER cwmsTsProfileParserView = AV_TS_PROFILE_PARSER.AV_TS_PROFILE_PARSER;

    public TimeSeriesProfileParserDao(DSLContext dsl) {
        super(dsl);
    }

    protected static List<ParameterInfo> getParameterInfoList(String info, String recordDelimiter, String fieldDelimiter) {
        fieldDelimiter = fieldDelimiter == null ? "," : fieldDelimiter;
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        String[] records = info.split(recordDelimiter);
        for (String aRecord : records) {
            String[] fields = aRecord.split(fieldDelimiter);
            int index;
            if (fields[2].isEmpty()) {
                index = Integer.parseInt(fields[3]);
            } else {
                index = Integer.parseInt(fields[2]);
            }
            ParameterInfo parameterInfo = new ParameterInfoIndexed.Builder().withIndex(index)
                    .withParameter(fields[0])
                    .withUnit(fields[1])
                    .build();
            parameterInfoList.add(parameterInfo);
        }
        return parameterInfoList;
    }

    private String getParameterInfoString(TimeSeriesProfileParser timeSeriesProfileParser) {
        List<ParameterInfo> parameterInfo = timeSeriesProfileParser.getParameterInfoList();

        StringBuilder parameterInfoBuilder = new StringBuilder();
        parameterInfoBuilder.append(parameterInfo.get(0).getParameterInfoString());

        for (int i = 1; i < parameterInfo.size(); i++) {
            parameterInfoBuilder.append(timeSeriesProfileParser.getRecordDelimiter())
                    .append(parameterInfo.get(i).getParameterInfoString());
        }
        return parameterInfoBuilder.toString();
    }

    public void storeTimeSeriesProfileParser(TimeSeriesProfileParserIndexed timeSeriesProfileParser,
            boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, timeSeriesProfileParser.getLocationId().getOfficeId());
            CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_PARSER(DSL.using(conn).configuration(),
                    timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(),
                    String.valueOf(timeSeriesProfileParser.getRecordDelimiter()),
                    String.valueOf(timeSeriesProfileParser.getFieldDelimiter()), timeSeriesProfileParser.getTimeField(),
                    null, null, timeSeriesProfileParser.getTimeFormat(),
                    timeSeriesProfileParser.getTimeZone(), getParameterInfoString(timeSeriesProfileParser),
                    timeSeriesProfileParser.getTimeInTwoFields() ? "T" : "F",
                    failIfExists ? "T" : "F", "T",
                    timeSeriesProfileParser.getLocationId().getOfficeId());
        });
    }

    public void storeTimeSeriesProfileParser(TimeSeriesProfileParserColumnar timeSeriesProfileParser,
            boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, timeSeriesProfileParser.getLocationId().getOfficeId());
            CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE_PARSER(DSL.using(conn).configuration(),
                    timeSeriesProfileParser.getLocationId().getName(),
                    timeSeriesProfileParser.getKeyParameter(),
                    String.valueOf(timeSeriesProfileParser.getRecordDelimiter()),
                    null, null,
                    timeSeriesProfileParser.getTimeStartColumn(), timeSeriesProfileParser.getTimeEndColumn(),
                    timeSeriesProfileParser.getTimeFormat(),
                    timeSeriesProfileParser.getTimeZone(), getParameterInfoString(timeSeriesProfileParser),
                    timeSeriesProfileParser.getTimeInTwoFields() ? "T" : "F",
                    failIfExists ? "T" : "F", "F",
                    timeSeriesProfileParser.getLocationId().getOfficeId());
        });
    }

    public TimeSeriesProfileParser retrieveTimeSeriesProfileParser(String locationId, String parameterId,
            String officeId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            RETRIEVE_TS_PROFILE_PARSER timeSeriesProfileParser
                    = CWMS_TS_PROFILE_PACKAGE.call_RETRIEVE_TS_PROFILE_PARSER(DSL.using(conn).configuration(),
                    locationId, parameterId, officeId);
            return map(timeSeriesProfileParser, locationId, parameterId, officeId);
        });
    }

    public List<ParameterInfo> retrieveParameterInfoList(String locationId, String parameterId, String officeId) {
        List<ParameterInfo> parameterInfoList = new ArrayList<>();
        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(cwmsTsProfileParserParamView.LOCATION_ID, locationId);
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(cwmsTsProfileParserParamView.OFFICE_ID, officeId));
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(cwmsTsProfileParserParamView.KEY_PARAMETER_ID, parameterId));
        Result<Record> parameterInfoResults = dsl.select(DSL.asterisk()).from(cwmsTsProfileParserParamView)
                .where(whereCondition)
                .fetch();
        for (Record recordParameterInfo : parameterInfoResults) {
            Short parameterField = cwmsTsProfileParserParamView.PARAMETER_FIELD.get(recordParameterInfo);
            if (parameterField != null) {
                parameterInfoList.add(new ParameterInfoIndexed.Builder()
                        .withIndex(parameterField)
                        .withParameter(cwmsTsProfileParserParamView.PARAMETER_ID.get(recordParameterInfo))
                        .withUnit(cwmsTsProfileParserParamView.PARAMETER_UNIT.get(recordParameterInfo))
                        .build());
            } else {
                parameterInfoList.add(new ParameterInfoColumnar.Builder()
                        .withStartColumn(cwmsTsProfileParserParamView.PARAMETER_COL_START.get(recordParameterInfo))
                        .withEndColumn(cwmsTsProfileParserParamView.PARAMETER_COL_END.get(recordParameterInfo))
                        .withParameter(cwmsTsProfileParserParamView.PARAMETER_ID.get(recordParameterInfo))
                        .withUnit(cwmsTsProfileParserParamView.PARAMETER_UNIT.get(recordParameterInfo))
                        .build());
            }
        }
        return parameterInfoList;
    }

    public List<TimeSeriesProfileParser> catalogTimeSeriesProfileParsers(String locationIdMask, String parameterIdMask,
            String officeIdMask, boolean includeParameters) {
        List<TimeSeriesProfileParser> timeSeriesProfileParserList = new ArrayList<>();

        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(cwmsTsProfileParserView.LOCATION_ID, locationIdMask);
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(cwmsTsProfileParserView.OFFICE_ID, officeIdMask));
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(cwmsTsProfileParserView.KEY_PARAMETER_ID, parameterIdMask));

        @NotNull Result<Record> timeSeriesProfileParserResults = dsl.select(DSL.asterisk())
                .from(cwmsTsProfileParserView)
                .where(whereCondition)
                .fetch();
        for (Record profileParser : timeSeriesProfileParserResults) {
            String recordDelimiter = cwmsTsProfileParserView.RECORD_DELIMTER_VALUE.get(profileParser);
            String fieldDelimiter = cwmsTsProfileParserView.FIELD_DELIMIETER_VALUE.get(profileParser);
            Short timeField = cwmsTsProfileParserView.TIME_FIELD.get(profileParser);
            Short timeStartCol = cwmsTsProfileParserView.TIME_COL_START.get(profileParser);
            Short timeEndCol = cwmsTsProfileParserView.TIME_COL_END.get(profileParser);
            CwmsId locationId = new CwmsId.Builder()
                    .withOfficeId(cwmsTsProfileParserView.OFFICE_ID.get(profileParser))
                    .withName(cwmsTsProfileParserView.LOCATION_ID.get(profileParser))
                    .build();
            String keyParameter = cwmsTsProfileParserView.KEY_PARAMETER_ID.get(profileParser);
            TimeSeriesProfileParser timeSeriesProfileParser;
            List<ParameterInfo> parameterInfoList = null;
            if (includeParameters) {
                parameterInfoList = retrieveParameterInfoList(locationId.getName(), keyParameter,
                        locationId.getOfficeId());
            }
            if (timeField != null) {
                timeSeriesProfileParser = new TimeSeriesProfileParserIndexed.Builder()
                        .withFieldDelimiter(fieldDelimiter.toCharArray()[0])
                        .withTimeField(timeField)
                        .withLocationId(locationId)
                        .withKeyParameter(keyParameter)
                        .withParameterInfoList(parameterInfoList)
                        .withTimeFormat(cwmsTsProfileParserView.TIME_FORMAT.get(profileParser))
                        .withTimeZone(cwmsTsProfileParserView.TIME_ZONE_ID.get(profileParser))
                        .withRecordDelimiter(recordDelimiter.toCharArray()[0])
                        .build();
                timeSeriesProfileParserList.add(timeSeriesProfileParser);
            } else if (timeStartCol != null && timeEndCol != null) {
                timeSeriesProfileParser = new TimeSeriesProfileParserColumnar.Builder()
                        .withTimeStartColumn(timeStartCol)
                        .withTimeEndColumn(timeEndCol)
                        .withLocationId(locationId)
                        .withKeyParameter(keyParameter)
                        .withParameterInfoList(parameterInfoList)
                        .withTimeFormat(cwmsTsProfileParserView.TIME_FORMAT.get(profileParser))
                        .withTimeZone(cwmsTsProfileParserView.TIME_ZONE_ID.get(profileParser))
                        .withRecordDelimiter(recordDelimiter.toCharArray()[0])
                        .build();
                timeSeriesProfileParserList.add(timeSeriesProfileParser);
            }
        }
        return timeSeriesProfileParserList;
    }

    public void deleteTimeSeriesProfileParser(String locationId, String parameterId, String officeId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE_PARSER(DSL.using(conn).configuration(), locationId,
                    parameterId, officeId);
        });
    }

    private TimeSeriesProfileParser map(RETRIEVE_TS_PROFILE_PARSER timeSeriesProfileParser, String locationName,
            String keyParameter, String officeId) {
        String info = timeSeriesProfileParser.getP_PARAMETER_INFO();
        List<ParameterInfo> parameterInfo = getParameterInfoList(info, timeSeriesProfileParser.getP_RECORD_DELIMITER(),
                timeSeriesProfileParser.getP_FIELD_DELIMITER());
        CwmsId locationId = new CwmsId.Builder().withOfficeId(officeId).withName(locationName).build();
        if (timeSeriesProfileParser.getP_TIME_FIELD() != null
                && timeSeriesProfileParser.getP_FIELD_DELIMITER() != null) {
            return new TimeSeriesProfileParserIndexed.Builder()
                    .withTimeField(timeSeriesProfileParser.getP_TIME_FIELD())
                    .withFieldDelimiter(timeSeriesProfileParser.getP_FIELD_DELIMITER().toCharArray()[0])
                    .withLocationId(locationId)
                    .withTimeZone(timeSeriesProfileParser.getP_TIME_ZONE())
                    .withTimeFormat(timeSeriesProfileParser.getP_TIME_FORMAT())
                    .withKeyParameter(keyParameter)
                    .withRecordDelimiter(timeSeriesProfileParser.getP_RECORD_DELIMITER().toCharArray()[0])
                    .withParameterInfoList(parameterInfo)
                    .build();
        } else if (timeSeriesProfileParser.getP_TIME_COL_START() != null
                && timeSeriesProfileParser.getP_TIME_COL_END() != null) {
            return new TimeSeriesProfileParserColumnar.Builder()
                    .withTimeStartColumn(timeSeriesProfileParser.getP_TIME_COL_START())
                    .withTimeEndColumn(timeSeriesProfileParser.getP_TIME_COL_END())
                    .withLocationId(locationId)
                    .withTimeZone(timeSeriesProfileParser.getP_TIME_ZONE())
                    .withTimeFormat(timeSeriesProfileParser.getP_TIME_FORMAT())
                    .withKeyParameter(keyParameter)
                    .withRecordDelimiter(timeSeriesProfileParser.getP_RECORD_DELIMITER().toCharArray()[0])
                    .withParameterInfoList(parameterInfo)
                    .build();
        }
        return new TimeSeriesProfileParser.Builder()
                .withLocationId(locationId)
                .withTimeZone(timeSeriesProfileParser.getP_TIME_ZONE())
                .withTimeFormat(timeSeriesProfileParser.getP_TIME_FORMAT())
                .withKeyParameter(keyParameter)
                .withRecordDelimiter(timeSeriesProfileParser.getP_RECORD_DELIMITER().toCharArray()[0])
                .withTimeInTwoFields(false)
                .withParameterInfoList(parameterInfo)
                .build();
    }
}
