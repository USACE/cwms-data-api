package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CREATE_AS_LRTS;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.DATE_FORMAT;
import static cwms.cda.api.Controllers.DATUM;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.END_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.EXAMPLE_DATE;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.MAX_VERSION;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.START_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.STORE_RULE;
import static cwms.cda.api.Controllers.TIMESERIES;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.queryParamAsZdt;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.api.Controllers.requiredZdt;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

public class TimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TimeSeriesController.class.getName());

    public static final String TAG = "TimeSeries";
    public static final String STORE_RULE_DESC = "The business rule to use "
            + "when merging the incoming with existing data\n"
            + "<table  border=\"1\" summary=\"\">\n"
            + "<tr><td colspan=2>Store Rules</td></tr>\n"
            + "<tr>\n"
            + "    <td>Delete Insert</td>\n"
            + "    <td>All existing data in the time window will be deleted and "
            + "then replaced with the new dataset.</td>\n"
            + "</tr>\n"
            + "<tr>\n"
            + "    <td>Replace All</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>When the new dataset's date/time exactly matches the date/time of "
            + "an existing data value, the new data value will replace the existing data.</li>\n"
            + "        <li>When the new dataset's data/time does not match an existing data/time "
            + "(i.e., a new data/time - data value pair) then an insert to the database "
            + "will occur.</li>\n"
            + "            <li>When there's an existing \"data/time - data value pair\" without "
            + "a corresponding date/time value pair, no change will happen to the existing "
            + "date/time value pair.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "</tr>\n"
            + "<tr>\n"
            + "    <td>Replace With Non Missing</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>New data is always inserted, i.e., an existing date/time-value "
            + "pair does not already exist for the record.</li>\n"
            + "            <li>If date/time-value pair does exist, then only non-missing value "
            + "will replace the existing data value*.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "<tr>\n"
            + "    <td>Replace Missing Values Only</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>New data is always inserted, i.e., an existing date/time-value "
            + "pair does not already exist for the record.</li>\n"
            + "            <li>If date/time-value pair does exist, then only replace an existing "
            + "data/time-value pair whose missing flag was set.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "<tr>\n"
            + "    <td>Do Not Replace</td>\n"
            + "    <td>\n"
            + "        Only inserts new data values if an existing date/time-value pair does not "
            + "already exist.\n"
            + "        Note: an existing date/time-value pair whose missing value quality bit is "
            + "set will NOT be overwritten.\n"
            + "    </td>\n"
            + "</tr>\n"
            + "</table>";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;
    private static final int DEFAULT_PAGE_SIZE = 500;


    public TimeSeriesController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    static {
        JavalinValidation.register(StoreRule.class, StoreRule::getStoreRule);
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            description = "Used to create and save time-series data. Data to be stored must have "
                    + "time stamps in UTC represented as epoch milliseconds ",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                        @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2)
                    },
                    required = true
            ),
            queryParams = {
                @OpenApiParam(name = TIMEZONE, description = "Specifies "
                        + "the time zone of the version-date field (unless "
                        + "otherwise specified). If this field is not specified, the default time zone "
                        + "of UTC shall be used.\r\nIgnored if version-date was specified with "
                        + "offset and timezone."),
                @OpenApiParam(name = CREATE_AS_LRTS,  type = Boolean.class, description = "Flag indicating if "
                        + "timeseries should be created as Local Regular Time Series. "
                        + "'True' or 'False', default is 'False'"),
                @OpenApiParam(name = STORE_RULE, type = StoreRule.class,  description = STORE_RULE_DESC),
                @OpenApiParam(name = OVERRIDE_PROTECTION,  type = Boolean.class, description = "A flag "
                        + "to ignore the protected data quality when storing data. 'True' or 'False'")
            },
            method = HttpMethod.POST,
            path = "/timeseries",
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {
        boolean createAsLrts = ctx.queryParamAsClass(CREATE_AS_LRTS, Boolean.class)
                .getOrDefault(false);
        StoreRule storeRule = ctx.queryParamAsClass(STORE_RULE, StoreRule.class)
                .getOrDefault(StoreRule.REPLACE_ALL);
        boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class)
                .getOrDefault(TimeSeriesDaoImpl.OVERRIDE_PROTECTION);

        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);
            dao.create(timeSeries, createAsLrts, storeRule, overrideProtection);
            ctx.status(HttpServletResponse.SC_OK);
        } catch (IOException | DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @NotNull
    protected TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
        return new TimeSeriesDaoImpl(dsl, metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = TIMESERIES, required = true, description = "The timeseries-id of "
                    + "the timeseries values to be deleted. "),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of "
                    + "the timeseries to be deleted."),
            @OpenApiParam(name = BEGIN, required = true, description = "The start of the time "
                    + "window to delete. The format for this field is ISO 8601 extended, with "
                    + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                    + EXAMPLE_DATE + "'."),
            @OpenApiParam(name = END, required = true, description = "The end of the time "
                    + "window to delete.The format for this field is ISO 8601 extended, with "
                    + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                    + EXAMPLE_DATE + "'."),
            @OpenApiParam(name = TIMEZONE, description = "This field specifies a default timezone "
                    + "to be used if the format of the " + BEGIN + ", " + END + ", or "
                    + VERSION_DATE + " parameters do not include offset or time zone information. "
                    + "Defaults to UTC."),
            @OpenApiParam(name = VERSION_DATE, description = "The version date/time of the time "
                    + "series in the specified or default time zone. If NULL, the earliest or "
                    + "latest version date will be used depending on p_max_version."),
            @OpenApiParam(name = START_TIME_INCLUSIVE, type = Boolean.class, description = "A flag "
                    + "specifying whether any data at the start time should be deleted ('True') "
                    + "or only data <b><em>after</em></b> the start time ('False').  "
                    + "Default value is True"),
            @OpenApiParam(name = END_TIME_INCLUSIVE, type = Boolean.class, description = "A flag "
                    + "('True'/'False') specifying whether any data at the end time should be "
                    + "deleted ('True') or only data <b><em>before</em></b> the end time ('False'). "
                    + "Default value is False"),
            @OpenApiParam(name = MAX_VERSION, type = Boolean.class, description = "A flag "
                    + "('True'/'False') specifying whether to use the earliest ('False') or "
                    + "latest ('True') version date for each time if p_version_date is NULL.  "
                    + "Default is 'True'"),
            @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "A flag "
                    + "('True'/'False') specifying whether to delete protected data. "
                    + "Default is False")
        },
        method = HttpMethod.DELETE,
        path = "/timeseries/{timeseries}",
        tags = TAG
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String timeseries) {

        String office = requiredParam(ctx, OFFICE);

        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            Timestamp startTimeDate = Timestamp.from(requiredZdt(ctx, BEGIN).toInstant());
            Timestamp endTimeDate = Timestamp.from(requiredZdt(ctx, END).toInstant());

            Timestamp versionDate = null;
            ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);
            if (versionZdt != null) {
                versionDate = Timestamp.from(versionZdt.toInstant());
            }

            // FYI queryParamAsClass with Boolean.class returns a case-insensitive comparison to "true".
            boolean startTimeInclusive = ctx.queryParamAsClass(START_TIME_INCLUSIVE, Boolean.class)
                    .getOrDefault(true);
            boolean endTimeInclusive = ctx.queryParamAsClass(END_TIME_INCLUSIVE, Boolean.class)
                    .getOrDefault(false);
            boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, Boolean.class)
                    .getOrDefault(true);
            boolean opArg = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class)
                    .getOrDefault(false);

            TimeSeriesDaoImpl.OverrideProtection op;
            if (opArg) {
                op = TimeSeriesDaoImpl.OverrideProtection.True;
            } else {
                op = TimeSeriesDaoImpl.OverrideProtection.False;
            }

            TimeSeriesDeleteOptions options = new TimeSeriesDaoImpl.DeleteOptions.Builder()
                    .withStartTime(startTimeDate)
                    .withEndTime(endTimeDate)
                    .withVersionDate(versionDate)
                    .withStartTimeInclusive(startTimeInclusive)
                    .withEndTimeInclusive(endTimeInclusive)
                    .withMaxVersion(maxVersion)
                    .withOverrideProtection(op.toString())
                    .build();
            dao.delete(office, timeseries, options);
        }
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                        + "name(s) of the time series whose data is to be included in the "
                        + "response. A case insensitive comparison is used to match names."),
                @OpenApiParam(name = OFFICE,  description = "Specifies the"
                        + " owning office of the time series(s) whose data is to be included "
                        + "in the response. "
                        + "Required for:" + Formats.JSONV2 + " and " + Formats.XMLV2 + ". "
                        + "For other formats, if this field is not specified, matching location "
                        + "level information from all offices shall be returned."),
                @OpenApiParam(name = UNIT,  description = "Specifies the "
                        + "unit or unit system of the response. Valid values for the unit "
                        + "field are:\r\n 1. EN.   (default) Specifies English unit system.  "
                        + "Location level values will be in the default English units for "
                        + "their parameters.\r\n2. SI.   Specifies the SI unit system.  "
                        + "Location level values will be in the default SI units for their "
                        + "parameters.\r\n3. Other. Any unit returned in the response to the "
                        + "units URI request that is appropriate for the requested parameters"
                        + "."),
                @OpenApiParam(name = VERSION_DATE, description = "Specifies the version date of a "
                        + "time series trace to be selected. The format for this field is ISO 8601 "
                        + "extended, i.e., 'format', e.g., '2021-06-10T13:00:00-0700' .If field is "
                        + "empty, query will return a max aggregate for the timeseries. "
                        + "Only supported for:" + Formats.JSONV2 + " and " + Formats.XMLV2),
                @OpenApiParam(name = DATUM,  description = "Specifies the "
                        + "elevation datum of the response. This field affects only elevation"
                        + " location levels. Valid values for this field are:\r\n1. NAVD88.  "
                        + "The elevation values will in the specified or default units above "
                        + "the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in "
                        + "the specified or default units above the NGVD-29 datum.  "
                        + "This parameter is not supported for:" + Formats.JSONV2 + " or " + Formats.XMLV2),
                @OpenApiParam(name = BEGIN,  description = "Specifies the "
                        + "start of the time window for data to be included in the response. "
                        + "If this field is not specified, any required time window begins 24"
                        + " hours prior to the specified or default end time. The format for "
                        + "this field is ISO 8601 extended, with optional offset and "
                        + "timezone, i.e., '"
                        + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                @OpenApiParam(name = END,  description = "Specifies the "
                        + "end of the time window for data to be included in the response. If"
                        + " this field is not specified, any required time window ends at the"
                        + " current time. The format for this field is ISO 8601 extended, "
                        + "with optional timezone, i.e., '"
                        + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                        + "the time zone of the values of the begin and end fields (unless "
                        + "otherwise specified).  "
                        + "For " + Formats.JSONV2 + " and " + Formats.XMLV2
                        + " the results are returned in UTC.  For other formats this parameter "
                        + "affects the time zone of times in the "
                        + "response. If this field is not specified, the default time zone "
                        + "of UTC shall be used.\r\nIgnored if begin was specified with "
                        + "offset and timezone."),
                @OpenApiParam(name = Controllers.TRIM, type = Boolean.class, description = "Specifies "
                        + "whether to trim missing values from the beginning and end of the "
                        + "retrieved values. "
                        + "Only supported for:" + Formats.JSONV2 + " and " + Formats.XMLV2 + ". "
                        + "Default is false."),
                @OpenApiParam(name = FORMAT,  description = "Specifies the"
                        + " encoding format of the response. Valid values for the format "
                        + "field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    "
                        + "xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json "
                        + "(default)"),
                @OpenApiParam(name = PAGE, description = "This end point can return large amounts "
                        + "of data as a series of pages. This parameter is used to describes the "
                        + "current location in the response stream.  This is an opaque "
                        + "value, and can be obtained from the 'next-page' value in the response."),
                @OpenApiParam(name = PAGE_SIZE,
                        type = Integer.class,
                        description = "How many entries per page returned. "
                                + "Default " + DEFAULT_PAGE_SIZE + ".")
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                    description = "A list of elements of the data set you've selected.",
                    content = {
                        @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                        @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2),
                        @OpenApiContent(from = TimeSeries.class, type = Formats.XML),
                        @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                        @OpenApiContent(from = TimeSeries.class, type = ""),}),
                @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                        + "parameters did not find a timeseries."),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                        + "implemented")
            },
            method = HttpMethod.GET,
            path = "/timeseries",
            tags = TAG
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String names = requiredParam(ctx, NAME);

            String unit = ctx.queryParamAsClass(UNIT, String.class)
                    .getOrDefault(UnitSystem.EN.getValue());
            String datum = ctx.queryParam(DATUM);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                    .getOrDefault("UTC");
            Validator<Boolean> trim = ctx.queryParamAsClass(Controllers.TRIM, Boolean.class);

            ZonedDateTime versionDate = queryParamAsZdt(ctx, VERSION_DATE);

            // The following parameters are only used for jsonv2 and xmlv2
            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(TimeSeriesController.class.getName(),
                            GET_ALL));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE  },
                    Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(TimeSeriesController.class.getName(), GET_ALL));

            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, format);

            String results;
            String version = contentType.getParameters().get(VERSION);

            ZoneId tz = ZoneId.of(timezone, ZoneId.SHORT_IDS);
            begin = begin != null ? begin : "PT-24H";

            ZonedDateTime beginZdt = DateUtils.parseUserDate(begin, timezone);
            ZonedDateTime endZdt = end != null
                    ? DateUtils.parseUserDate(end, timezone)
                    : ZonedDateTime.now(tz);

            if (version != null && version.equals("2")) {

                if (datum != null) {
                    throw new IllegalArgumentException(String.format("Datum is not supported for:%s and %s",
                            Formats.JSONV2, Formats.XMLV2));
                }

                String office = requiredParam(ctx, OFFICE);
                TimeSeries ts = dao.getTimeseries(cursor, pageSize, names, office, unit,
                        beginZdt, endZdt, versionDate, trim.getOrDefault(false));

                results = Formats.format(contentType, ts);

                ctx.status(HttpServletResponse.SC_OK);

                // Send back the link to the next page in the response header
                StringBuilder linkValue = new StringBuilder(600);
                linkValue.append(String.format("<%s>; rel=self; type=\"%s\"",
                        buildRequestUrl(ctx, ts, ts.getPage()), contentType));

                if (ts.getNextPage() != null) {
                    linkValue.append(",");
                    linkValue.append(String.format("<%s>; rel=next; type=\"%s\"",
                            buildRequestUrl(ctx, ts, ts.getNextPage()),
                            contentType));
                }

                ctx.header("Link", linkValue.toString());
                ctx.result(results).contentType(contentType.toString());
            } else {
                if (versionDate != null) {
                    throw new IllegalArgumentException(String.format("Version date is only supported for:%s and %s",
                            Formats.JSONV2, Formats.XMLV2));
                }

                if (trim.hasValue()) {
                    throw new IllegalArgumentException(String.format("Trim is only supported for:%s and %s",
                            Formats.JSONV2, Formats.XMLV2));
                }

                if (format == null || format.isEmpty()) {
                    format = "json";
                }

                String office = ctx.queryParam(OFFICE);
                results = dao.getTimeseries(format, names, office, unit, datum, beginZdt, endZdt, tz);
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);
            }
            requestResultSize.update(results.length());
        } catch (NotFoundException e) {
            CdaError re = new CdaError("Not found.");
            logger.log(Level.WARNING, re.toString(), e);
            ctx.status(HttpServletResponse.SC_NOT_FOUND);
            ctx.json(re);
        } catch (IllegalArgumentException ex) {
            CdaError re = new CdaError("Invalid arguments supplied");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_BAD_REQUEST);
            ctx.json(re);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String id) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }

    }

    @OpenApi(
            description = "Update a TimeSeries with provided values",
            pathParams = {
                @OpenApiParam(name = TIMESERIES, description = "Full CWMS Timeseries name")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                        @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2)
                    },
                    required = true),
            queryParams = {
                @OpenApiParam(name = TIMEZONE, description = "Specifies "
                        + "the time zone of the version-date field (unless "
                        + "otherwise specified). If this field is not specified, the default time zone "
                        + "of UTC shall be used.\r\nIgnored if version-date was specified with "
                        + "offset and timezone."),
                @OpenApiParam(name = CREATE_AS_LRTS, type = Boolean.class, description = ""),
                @OpenApiParam(name = STORE_RULE,  type = StoreRule.class, description = STORE_RULE_DESC),
                @OpenApiParam(name = OVERRIDE_PROTECTION,  type = Boolean.class, description =
                        "A flag to ignore the protected data quality when storing data.  \"'true' or 'false'\"")
            },
            method = HttpMethod.PATCH,
            path = "/timeseries/{timeseries}",
            tags = TAG
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {

        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);

            boolean createAsLrts = ctx.queryParamAsClass(CREATE_AS_LRTS, Boolean.class)
                    .getOrDefault(false);
            StoreRule storeRule = ctx.queryParamAsClass(STORE_RULE, StoreRule.class)
                    .getOrDefault(StoreRule.REPLACE_ALL);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class)
                    .getOrDefault(TimeSeriesDaoImpl.OVERRIDE_PROTECTION);

            dao.store(timeSeries, createAsLrts, storeRule, overrideProtection);

            ctx.status(HttpServletResponse.SC_OK);
        } catch (IOException | DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private TimeSeries deserializeTimeSeries(Context ctx) throws IOException {
        return deserializeTimeSeries(ctx.bodyAsInputStream(), getUserDataContentType(ctx).toString());
    }

    public static TimeSeries deserializeTimeSeries(InputStream inputStream, String contentType)
            throws IOException {
        TimeSeries retval;

        if ((Formats.XMLV2).equals(contentType)) {
            TimeSeries result;
            try {
                JAXBContext jaxbContext = JAXBContext.newInstance(TimeSeries.class);
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                result = (TimeSeries) unmarshaller.unmarshal(inputStream);
            } catch (JAXBException e) {
                throw new IOException(e);
            }
            retval = result;
        } else if ((Formats.JSONV2).equals(contentType)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(inputStream, TimeSeries.class);
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    private ContentType getUserDataContentType(@NotNull Context ctx) {
        String contentTypeHeader = ctx.req.getContentType();
        return Formats.parseHeader(contentTypeHeader);
    }

    /**
     * Builds a URL that references a specific "page" of the result.
     *
     * @param ctx the context of the request
     * @param ts the TimeSeries object that was used to generate the result
     * @return a URL that references the same query, but with a different "page" parameter
     */
    private String buildRequestUrl(Context ctx, TimeSeries ts, String cursor) {
        StringBuffer result = ctx.req.getRequestURL();
        try {
            result.append(String.format("?name=%s", URLEncoder.encode(ts.getName(),
                    StandardCharsets.UTF_8.toString())));
            result.append(String.format("&office=%s", URLEncoder.encode(ts.getOfficeId(),
                    StandardCharsets.UTF_8.toString())));
            result.append(String.format("&unit=%s", URLEncoder.encode(ts.getUnits(),
                    StandardCharsets.UTF_8.toString())));
            result.append(String.format("&begin=%s",
                    URLEncoder.encode(ts.getBegin().format(DateTimeFormatter.ISO_ZONED_DATE_TIME),
                            StandardCharsets.UTF_8.toString())));
            result.append(String.format("&end=%s",
                    URLEncoder.encode(ts.getEnd().format(DateTimeFormatter.ISO_ZONED_DATE_TIME),
                            StandardCharsets.UTF_8.toString())));

            String format = ctx.queryParam(FORMAT);
            if (format != null && !format.isEmpty()) {
                result.append(String.format("&format=%s", format));
            }

            if (cursor != null && !cursor.isEmpty()) {
                result.append(String.format("&page=%s", URLEncoder.encode(cursor,
                        StandardCharsets.UTF_8.toString())));
            }
        } catch (UnsupportedEncodingException ex) {
            // We shouldn't get here
            logger.log(Level.WARNING, null, ex);
        }
        return result.toString();
    }
}
