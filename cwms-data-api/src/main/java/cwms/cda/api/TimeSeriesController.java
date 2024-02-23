package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.Tsv;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.ws.Response;

import org.apache.http.HttpStatus;
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
            + "    <td>All existing data in the time window will be deleted and then replaced with the new dataset.</td>\n"
            + "</tr>\n"
            + "<tr>\n"
            + "    <td>Replace All</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>When the new dataset's date/time exactly matches the date/time of an existing data value, the new data value will replace the existing data.</li>\n"
            + "        <li>When the new dataset's data/time does not match an existing data/time (i.e., a new data/time - data value pair) then an insert to the database will occur.</li>\n"
            + "            <li>When there's an existing \"data/time - data value pair\" without a corresponding date/time value pair, no change will happen to the existing date/time value pair.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "</tr>\n"
            + "<tr>\n"
            + "    <td>Replace With Non Missing</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>New data is always inserted, i.e., an existing date/time-value pair does not already exist for the record.</li>\n"
            + "            <li>If date/time-value pair does exist, then only non-missing value will replace the existing data value*.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "<tr>\n"
            + "    <td>Replace Missing Values Only</td>\n"
            + "    <td>\n"
            + "        <ul>\n"
            + "            <li>New data is always inserted, i.e., an existing date/time-value pair does not already exist for the record.</li>\n"
            + "            <li>If date/time-value pair does exist, then only replace an existing data/time-value pair whose missing flag was set.</li>\n"
            + "        </ul>\n"
            + "    </td>\n"
            + "<tr>\n"
            + "    <td>Do Not Replace</td>\n"
            + "    <td>\n"
            + "        Only inserts new data values if an existing date/time-value pair does not already exist.\n"
            + "        Note: an existing date/time-value pair whose missing value quality bit is set will NOT be overwritten.\n"
            + "    </td>\n"
            + "</tr>\n"
            + "</table>";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;
    private final int defaultPageSize = 500;


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
            description = "Used to create and save time-series data. Data to be stored must have time stamps in UTC represented as epoch milliseconds ",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                            @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2)
                    },
                    required = true
            ),
            queryParams = {
                    @OpenApiParam(name = VERSION_DATE, description = "Specifies the version date for "
                            + "the timeseries to create. If this field is not specified, a null "
                            + "version date will be used.  "
                            + "The format for this field is ISO 8601 extended, with optional timezone, "
                            + "i.e., '" + FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies "
                            + "the time zone of the version-date field (unless "
                            + "otherwise specified). If this field is not specified, the default time zone "
                            + "of UTC shall be used.\r\nIgnored if version-date was specified with "
                            + "offset and timezone."),
                    @OpenApiParam(name = CREATE_AS_LRTS,  type = Boolean.class, description = "Flag indicating if "
                            + "timeseries should be created as Local Regular Time Series. "
                            + "'True' or 'False', default is 'False'"),
                    @OpenApiParam(name = STORE_RULE, type = StoreRule.class,  description = STORE_RULE_DESC),
                    @OpenApiParam(name = OVERRIDE_PROTECTION,  type = Boolean.class, description =
                            "A flag to ignore the protected data quality when storing data. "
                                    + "'True' or 'False'")
            },
            method = HttpMethod.POST,
            path = "/timeseries",
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {

        String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
        String version = ctx.queryParam(VERSION_DATE);
        Timestamp versionDate = TimeSeriesDao.NON_VERSIONED;
        if (version != null) {
            ZonedDateTime beginZdt = DateUtils.parseUserDate(version, timezone);
            versionDate = Timestamp.from(beginZdt.toInstant());
        }

        boolean createAsLrts = ctx.queryParamAsClass(CREATE_AS_LRTS, Boolean.class).getOrDefault(false);
        StoreRule storeRule = ctx.queryParamAsClass(STORE_RULE, StoreRule.class).getOrDefault(StoreRule.REPLACE_ALL);
        boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class).getOrDefault(TimeSeriesDaoImpl.OVERRIDE_PROTECTION);

        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);
            dao.create(timeSeries, versionDate, createAsLrts, storeRule, overrideProtection);
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
        return new TimeSeriesDaoImpl(dsl);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TIMESERIES, required = true, description = "The timeseries-id of the timeseries values to be deleted. "),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of the timeseries to be deleted."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window to delete. "
                            + "The format for this field is ISO 8601 extended, with optional offset and timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window to delete."
                    + "The format for this field is ISO 8601 extended, with optional offset and timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = TIMEZONE, description = "This field specifies a default timezone to be used if the format of the "
                            + BEGIN + ", " + END + ", or " + VERSION_DATE + " parameters do not include offset or time zone information. "
                            + "Defaults to UTC."),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date/time of the time series in the specified or default time zone. If NULL, the earliest or latest version date will be used depending on p_max_version."),
                    @OpenApiParam(name = START_TIME_INCLUSIVE, description = "A flag specifying whether any data at the start time should be deleted ('True') or only data <b><em>after</em></b> the start time ('False').  Default value is True", type = Boolean.class),
                    @OpenApiParam(name = END_TIME_INCLUSIVE, description = "A flag ('True'/'False') specifying whether any data at the end time should be deleted ('True') or only data <b><em>before</em></b> the end time ('False'). Default value is False", type = Boolean.class),
                    @OpenApiParam(name = MAX_VERSION, description = "A flag ('True'/'False') specifying whether to use the earliest ('False') or latest ('True') version date for each time if p_version_date is NULL.  Default is 'True'", type = Boolean.class),
                    @OpenApiParam(name = OVERRIDE_PROTECTION, description = "A flag ('True'/'False') specifying whether to delete protected data. Default is False", type = Boolean.class)
            },
            method = HttpMethod.DELETE,
            path = "/timeseries/{timeseries}",
            tags = {TAG}
    )
    @Override
    public void delete(Context ctx, @NotNull String timeseries) {

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");

            Timestamp startTimeDate = Timestamp.from(DateUtils.parseUserDate(ctx.queryParam(BEGIN), timezone).toInstant());
            Timestamp endTimeDate = Timestamp.from(DateUtils.parseUserDate(ctx.queryParam(END), timezone).toInstant());
            String versionDateParam = ctx.queryParam(VERSION_DATE);
            Timestamp versionDate = null;
            if (versionDateParam != null) {
                versionDate = Timestamp.from(DateUtils.parseUserDate(versionDateParam, timezone).toInstant());
            }

            // FYI queryParamAsClass with Boolean.class returns a case-insensitive comparison to "true".
            boolean startTimeInclusive = ctx.queryParamAsClass(START_TIME_INCLUSIVE, Boolean.class).getOrDefault(true);
            boolean endTimeInclusive = ctx.queryParamAsClass(END_TIME_INCLUSIVE, Boolean.class).getOrDefault(false);
            boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, Boolean.class).getOrDefault(true);
            boolean opArg = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class).getOrDefault(false);

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
                            + "in the response. If this field is not specified, matching location"
                            + " level information from all offices shall be returned."),
                    @OpenApiParam(name = UNIT,  description = "Specifies the "
                            + "unit or unit system of the response. Valid values for the unit "
                            + "field are:\r\n 1. EN.   (default) Specifies English unit system.  "
                            + "Location level values will be in the default English units for "
                            + "their parameters.\r\n2. SI.   Specifies the SI unit system.  "
                            + "Location level values will be in the default SI units for their "
                            + "parameters.\r\n3. Other. Any unit returned in the response to the "
                            + "units URI request that is appropriate for the requested parameters"
                            + "."),
                    @OpenApiParam(name = VERSION_DATE, description="Specifies the version date of a "
                            + "time series trace to be selected. The format for this field is ISO 8601 "
                            + "extended, i.e., 'format', e.g., '2021-06-10T13:00:00-0700' .If field is "
                            + "empty, query will return a max aggregate for the timeseries."),
                    @OpenApiParam(name = VERSION_TYPE,
                            type = VersionType.class,
                            description = VersionType.DESCRIPTION
                    ),
                    @OpenApiParam(name = DATUM,  description = "Specifies the "
                            + "elevation datum of the response. This field affects only elevation"
                            + " location levels. Valid values for this field are:\r\n1. NAVD88.  "
                            + "The elevation values will in the specified or default units above "
                            + "the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in "
                            + "the specified or default units above the NGVD-29 datum."),
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
                            + "otherwise specified), as well as the time zone of any times in the"
                            + " response. If this field is not specified, the default time zone "
                            + "of UTC shall be used.\r\nIgnored if begin was specified with "
                            + "offset and timezone."),
                    @OpenApiParam(name = FORMAT,  description = "Specifies the"
                            + " encoding format of the response. Valid values for the format "
                            + "field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    "
                            + "xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json "
                            + "(default)"),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entries per page returned. "
                                            + "Default " + defaultPageSize + "."
                    )
            },
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "A list of elements of the data set you've selected.",
                    content = {
                            @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                            @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2),
                            @OpenApiContent(from = TimeSeries.class, type = Formats.XML),
                            @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                            @OpenApiContent(from = TimeSeries.class, type = ""),
                    }
            ),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a timeseries."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            method = HttpMethod.GET,
            path = "/timeseries",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String names = ctx.queryParam(NAME);
            String office = ctx.queryParam(OFFICE);
            String unit = ctx.queryParamAsClass(UNIT, String.class)
                    .getOrDefault(UnitSystem.EN.getValue());
            String datum = ctx.queryParam(DATUM);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String versionDateParam = ctx.queryParam(VERSION_DATE);
            ZonedDateTime versionDate = null;
            if (versionDateParam != null) {
                versionDate = DateUtils.parseUserDate(versionDateParam, timezone);
            }

            String versionType = queryParamAsClass(ctx,
                    new String[]{VERSION_TYPE, },
                    String.class, VersionType.MAX_AGGREGATE.getValue(), metrics,
                    name(TimeSeriesController.class.getName(), GET_ONE));

            // Throw error if max aggregate version type is requested with a version date
            // or single version version type is requested without a version date
            if( (Objects.equals(versionType, "MAX_AGGREGATE") && !Objects.equals(versionDate, null))
                || (Objects.equals(versionType, "SINGLE_VERSION") && Objects.equals(versionDate, null)) ) {
                throw new IllegalArgumentException();
            }

            // The following parameters are only used for jsonv2 and xmlv2
            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(TimeSeriesController.class.getName(),
                            GET_ALL));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE  },
                    Integer.class, defaultPageSize, metrics,
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
                TimeSeries ts = dao.getTimeseries(cursor, pageSize, names, office, unit, datum,
                        beginZdt, endZdt, tz, versionDate);

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
                if (format == null || format.isEmpty()) {
                    format = "json";
                }

                results = dao.getTimeseries(format, names, office, unit, datum,
                        beginZdt, endZdt, tz);
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
                    required = true
            ),
            queryParams = {
                    @OpenApiParam(name = VERSION_DATE, description = "Specifies the "
                            + "version date for the timeseries to create. If"
                            + " this field is not specified, a null version date will be used.  The format for this field is ISO 8601 extended, "
                            + "with optional timezone, i.e., '"
                            + FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
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
            tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {

        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String version = ctx.queryParam(VERSION_DATE);
            Timestamp versionDate = TimeSeriesDao.NON_VERSIONED;
            if (version != null) {
                ZonedDateTime beginZdt = DateUtils.parseUserDate(version, timezone);
                versionDate = Timestamp.from(beginZdt.toInstant());
            }

            boolean createAsLrts = ctx.queryParamAsClass(CREATE_AS_LRTS, Boolean.class).getOrDefault(false);
            StoreRule storeRule = ctx.queryParamAsClass(STORE_RULE, StoreRule.class).getOrDefault(StoreRule.REPLACE_ALL);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class).getOrDefault(TimeSeriesDaoImpl.OVERRIDE_PROTECTION);

            dao.store(timeSeries, versionDate, createAsLrts, storeRule, overrideProtection);

            ctx.status(HttpServletResponse.SC_OK);
        } catch (IOException | DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private TimeSeries deserializeTimeSeries(Context ctx) throws IOException {
        return deserializeTimeSeries(ctx.body(), getUserDataContentType(ctx));
    }

    private TimeSeries deserializeTimeSeries(String body, ContentType contentType)
            throws IOException {
        return deserializeTimeSeries(body, contentType.toString());
    }

    public static TimeSeries deserializeTimeSeries(String body, String contentType)
            throws IOException {
        TimeSeries retval;

        if ((Formats.XMLV2).equals(contentType)) {
            // This is how it would be done if we could use jackson to parse the xml
            // it currently doesn't work because some jackson annotations
            // use certain naming conventions (e.g. "value-columns" vs "valueColumns")
            //  ObjectMapper om = buildXmlObjectMapper();
            //  retval = om.readValue(body, TimeSeries.class);
            retval = deserializeJaxb(body);
        } else if ((Formats.JSONV2).equals(contentType)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, TimeSeries.class);
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    public static TimeSeries deserializeJaxb(String body) throws IOException {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(TimeSeries.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return (TimeSeries) unmarshaller.unmarshal(new StringReader(body));
        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

    @NotNull
    public static ObjectMapper buildXmlObjectMapper() {
        ObjectMapper retval = new XmlMapper();
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        return retval;
    }

    @NotNull
    private ContentType getContentType(Context ctx) {
        String acceptHeader = ctx.req.getHeader(ACCEPT);
        String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
        ContentType contentType = Formats.parseHeader(formatHeader);
        if (contentType == null) {
            throw new FormattingException("Format header could not be parsed");
        }
        return contentType;
    }

    private ContentType getUserDataContentType(@NotNull Context ctx) {
        String contentTypeHeader = ctx.req.getContentType();
        return Formats.parseHeader(contentTypeHeader);
    }

    /**
     * Builds a URL that references a specific "page" of the result.
     *
     * @param ctx
     * @param ts
     * @return
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

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                            + "timeseries group(s) whose data is to be included in the response. "
                            + "If this field is not specified, matching timeseries groups "
                            + "information from all offices shall be returned."),
               },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, from = Tsv.class, type = Formats.JSON)}
                    ),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the timeseries group(s) were not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not "
                            + "implemented")
            },
            path = "/timeseries/recent",
            description = "Returns CWMS Timeseries Groups Data",
            tags = {TAG},
            method = HttpMethod.GET
    )
    public void getRecent(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime("getRecent")) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParamAsClass(CATEGORY_ID, String.class).allowNullable().get(); // !!! TODO document this param
            String groupId = ctx.pathParamAsClass(GROUP_ID, String.class).allowNullable().get();
            String tsIdsParam = ctx.queryParamAsClass(TS_IDS, String.class).allowNullable().get();// !!! TODO document this param

            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            gregorianCalendar.set(Calendar.HOUR, 0);
            gregorianCalendar.set(Calendar.MINUTE, 0);
            gregorianCalendar.set(Calendar.SECOND, 0);
            gregorianCalendar.set(Calendar.MILLISECOND, 0);

            gregorianCalendar.add(Calendar.HOUR, 24 * 14);
            Timestamp futureLimit = Timestamp.from(gregorianCalendar.toInstant());
            gregorianCalendar.add(Calendar.HOUR, 24 * -28);
            Timestamp pastLimit = Timestamp.from(gregorianCalendar.toInstant());

            boolean hasTsGroupInfo = categoryId != null && !categoryId.isEmpty()
                    && groupId != null && !groupId.isEmpty();
            List<String> tsIds = getTsIds(tsIdsParam);
            boolean hasTsIds = tsIds != null && !tsIds.isEmpty();

            List<RecentValue> latestValues = null;
            if (hasTsGroupInfo && hasTsIds) {
                // has both = this is an error
                CdaError re = new CdaError("Invalid arguments supplied, group has both "
                        + "Timeseries Group info and Timeseries IDs.");
                logger.log(Level.SEVERE, "{0} for request {1}", new Object[]{ re, ctx.fullUrl()});
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if (!hasTsGroupInfo && !hasTsIds) {
                // doesn't have either?  Just return empty results?
                CdaError re = new CdaError("Invalid arguments supplied, group has neither "
                        + "Timeseries Group info nor Timeseries IDs");
                logger.log(Level.SEVERE, "{0} for request {1}", new Object[]{ re, ctx.fullUrl()});
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if (hasTsGroupInfo) {
                // just group provided
                latestValues = dao.findRecentsInRange(office, categoryId, groupId, pastLimit,
                        futureLimit);
            } else if (hasTsIds) {
                latestValues = dao.findMostRecentsInRange(tsIds, pastLimit, futureLimit);
            }

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = Formats.format(contentType, latestValues, RecentValue.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    public static List<String> getTsIds(String tsIdsParam) {
        List<String> retval = null;

        if (tsIdsParam != null && !tsIdsParam.isEmpty()) {
            retval = new ArrayList<>();

            if (tsIdsParam.startsWith("[")) {
                tsIdsParam = tsIdsParam.substring(1);
            }

            if (tsIdsParam.endsWith("]")) {
                tsIdsParam = tsIdsParam.substring(0, tsIdsParam.length() - 1);
            }

            if (!tsIdsParam.isEmpty()) {
                final String regex = "\"[^\"]*\"|[^,]+";
                final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

                try (Scanner s = new Scanner(tsIdsParam)) {
                    List<String> matches = findAll(s, pattern).map(m -> m.group().trim())
                            .collect(Collectors.toList());
                    retval.addAll(matches);
                }
            }
        }

        return retval;
    }

    // This came from https://stackoverflow.com/questions/42961296/java-8-stream-emitting-a-stream/42978216#42978216
    public static Stream<MatchResult> findAll(Scanner s, Pattern pattern) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<MatchResult>(
                1000, Spliterator.ORDERED | Spliterator.NONNULL) {
            public boolean tryAdvance(Consumer<? super MatchResult> action) {
                if (s.findWithinHorizon(pattern, 0) != null) {
                    action.accept(s.match());
                    return true;
                } else {
                    return false;
                }
            }
        }, false);
    }
}
