package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.FilteredTimeSeriesParameters;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesRequestParameters;
import cwms.cda.data.dto.TimeSeries;

import cwms.cda.data.dto.filteredtimeseries.FilteredTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DateUtils;
import io.javalin.core.util.Header;
import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class TimeSeriesFilteredController implements Handler {
    private static final Logger logger = Logger.getLogger(TimeSeriesFilteredController.class.getName());
    public static final String TAG = "TimeSeries";

    private static final int DEFAULT_PAGE_SIZE = 500;

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public TimeSeriesFilteredController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    private DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    private TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
        return new TimeSeriesDaoImpl(dsl, metrics);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "name of the time series whose data is to be included in the "
                            + "response. A case insensitive comparison is used to match names."),
                    @OpenApiParam(name = OFFICE, description = "Specifies the"
                            + " owning office of the time series(s) whose data is to be included "
                            + "in the response. "
                            + "Required for:" + Formats.JSONV2 + " and " + Formats.XMLV2 + ". "
                            + "For other formats, if this field is not specified, matching location "
                            + "level information from all offices shall be returned."),
                    @OpenApiParam(name = UNIT, description = "Specifies the "
                            + "unit or unit system of the response. Valid values for the unit "
                            + "field are: "
                            + "\n* `EN`  (default) Specifies English unit system.  "
                            + "Location level values will be in the default English units for "
                            + "their parameters."
                            + "\n* `SI`  Specifies the SI unit system.  "
                            + "Location level values will be in the default SI units for their "
                            + "parameters."
                            + "\n* `Other`  Any unit returned in the response to the units URI "
                            + "request that is appropriate for the requested parameters."),
                    @OpenApiParam(name = VERSION_DATE, description = "Specifies the version date of a "
                            + "time series trace to be selected. " +
                            TIME_FORMAT_DESC +
                            " If field is empty, query will return a max aggregate for the timeseries. "
                            + "Only supported for:" + Formats.JSONV2 + " and " + Formats.XMLV2),
                    @OpenApiParam(name = BEGIN, description = "Specifies the "
                            + "start of the time window for data to be included in the response. "
                            + "If this field is not specified, any required time window begins 24"
                            + " hours prior to the specified or default end time. " +
                            TIME_FORMAT_DESC),
                    @OpenApiParam(name = END, description = "Specifies the "
                            + "end of the time window for data to be included in the response. If"
                            + " this field is not specified, any required time window ends at the"
                            + " current time. " +
                            TIME_FORMAT_DESC),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies "
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
                            + "Default is true."),
                    @OpenApiParam(name = INCLUDE_ENTRY_DATE, type = Boolean.class, description = "Specifies "
                            + "whether to include the data entry date of each value in the response. Including the data entry "
                            + "date will increase the size of the array containing each data value from three to four, "
                            + "changing the format of the response. Default is false."),
                    @OpenApiParam(name = MIN_VALUE, type = Double.class, description = "Specifies "
                            + "the minimum value to include in the results. Values below this threshold will be excluded."),
                    @OpenApiParam(name = MAX_VALUE, type = Double.class, description = "Specifies "
                            + "the maximum value to include in the results. Values above this threshold will be excluded."),
                    @OpenApiParam(name = FILTER_NULLS, type = Boolean.class, description = "Specifies "
                            + "whether to exclude null values from the results. Default is false."),
                    @OpenApiParam(name = QUERY, description = "Specifies "
                            + "an RSQL-like <a href=\"rsql.html\"> query string to filter the results.  " +
                            "Expressions may reference \"value, datetime, quality, data_entry_date\""),
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
    public void handle(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            String format = "";
            String names = requiredParam(ctx, NAME);

            String unit = ctx.queryParamAsClass(UNIT, String.class)
                    .getOrDefault(UnitSystem.EN.getValue());

            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                    .getOrDefault("UTC");
            Validator<Boolean> trim = ctx.queryParamAsClass(Controllers.TRIM, Boolean.class);

            ZonedDateTime versionDate = queryParamAsZdt(ctx, VERSION_DATE);

            boolean includeEntryDate = ctx.queryParamAsClass(INCLUDE_ENTRY_DATE, Boolean.class)
                    .getOrDefault(false);

            // The following parameters are only used for jsonv2 and xmlv2
            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(TimeSeriesController.class.getName(),
                            GET_ALL));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                    Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(TimeSeriesController.class.getName(), GET_ALL));

            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, format, TimeSeries.class);

            ZoneId tz = ZoneId.of(timezone, ZoneId.SHORT_IDS);
            begin = begin != null ? begin : "PT-24H";

            ZonedDateTime beginZdt = DateUtils.parseUserDate(begin, timezone);
            ZonedDateTime endZdt = end != null
                    ? DateUtils.parseUserDate(end, timezone)
                    : ZonedDateTime.now(tz);

            String office = requiredParam(ctx, OFFICE);

            FilteredTimeSeriesParameters ftsParams = FilteredTimeSeriesParameters.Builder.from(ctx)
                    .build();

            TimeSeriesRequestParameters tsParams = new TimeSeriesRequestParameters.Builder()
                    .withNames(names)
                    .withOffice(office)
                    .withUnits(unit)
                    .withBeginTime(beginZdt)
                    .withEndTime(endZdt)
                    .withVersionDate(versionDate)
                    .withShouldTrim(trim.getOrDefault(true))
                    .withIncludeEntryDate(includeEntryDate)
                    .build();


            FilteredTimeSeries fts = dao.getTimeseries(cursor, pageSize, tsParams, ftsParams);

            String results = Formats.format(contentType, fts);

            ctx.status(HttpServletResponse.SC_OK);

            addLinkHeader(ctx, fts, contentType);

            ctx.result(results).contentType(contentType.toString());

            addDeprecatedContentTypeWarning(ctx, contentType);
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

    private void addLinkHeader(@NotNull Context ctx, FilteredTimeSeries fts, ContentType contentType) {
        // Send back the link to the next page in the response header
        try {
            // Send back the link to the next page in the response header
            StringBuilder linkValue = new StringBuilder(600);
            String pageUrl = buildRequestUrl(ctx, fts.getPage());
            linkValue.append(String.format("<%s>; rel=self; type=\"%s\"",
                    pageUrl, contentType));

            if (fts.getNextPage() != null) {
                linkValue.append(",");
                String nextPageUrl = buildRequestUrl(ctx, fts.getNextPage());
                linkValue.append(String.format("<%s>; rel=next; type=\"%s\"",
                        nextPageUrl,
                        contentType));
            }

            ctx.header("Link", linkValue.toString());
        } catch (URISyntaxException ex) {
            logger.log(Level.WARNING, null, ex);
        }
    }

    public String buildRequestUrl(Context ctx, String cursor) throws URISyntaxException {
        URIBuilder builder = new URIBuilder(ctx.req.getRequestURL().toString()); // requestURL stops just before ?
        builder.setParameters(URLEncodedUtils.parse(ctx.req.getQueryString(), StandardCharsets.UTF_8));

        // override or add the paging cursor
        if (cursor != null && !cursor.isEmpty()) {
            builder.setParameter("page", cursor);
        }

        return builder.build().toString();
    }


}
