package cwms.radar.api;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators;
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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.TimeSeriesDao;
import cwms.radar.data.dao.TimeSeriesDaoImpl;
import cwms.radar.data.dto.RecentValue;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.data.dto.Tsv;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.json.JsonV1;
import cwms.radar.helpers.DateUtils;
import cwms.radar.security.CwmsAuthorizer;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import static com.codahale.metrics.MetricRegistry.name;

public class TimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TimeSeriesController.class.getName());

    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Meter getRecentRequests;
    private final Timer getRecentRequestsTime;
    private final Meter createRequests;
    private final Timer createRequestsTime;
    private final Meter updateRequests;
    private final Timer updateRequestsTime;
    private final Meter deleteRequests;
    private final Timer deleteRequestsTime;

    private final Histogram requestResultSize;
    private final int defaultPageSize = 500;


    public TimeSeriesController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
        getRecentRequests = this.metrics.meter(name(className,"getRecent","count"));
        getRecentRequestsTime = this.metrics.timer(name(className,"getRecent","time"));
        createRequests = this.metrics.meter(name(className,"create","count"));
        createRequestsTime = this.metrics.timer(name(className,"create","time"));
        updateRequests = this.metrics.meter(name(className,"update","count"));
        updateRequestsTime = this.metrics.timer(name(className,"update","time"));
        deleteRequests = this.metrics.meter(name(className,"delete","count"));
        deleteRequestsTime = this.metrics.timer(name(className,"delete","time"));
    }

    @OpenApi(
            description = "Create new TimeSeries",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                            @OpenApiContent(from = TimeSeries.class, type = Formats.XML )
                    },
                    required = true
            ),
            method = HttpMethod.POST,
            path = "/timeseries",
            tags = {"TimeSeries"}
    )
    @Override
    public void create(Context ctx)
    {
        createRequests.mark();

        try(final Timer.Context timeContext = createRequestsTime.time(); DSLContext dsl = getDslContext(ctx))
        {
            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);
            dao.create(timeSeries);
            ctx.status(HttpServletResponse.SC_OK);
        }
        catch(IOException | DataAccessException ex)
        {
            RadarError re = new RadarError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    protected DSLContext getDslContext(Context ctx)
    {
        return JooqDao.getDslContext(ctx);
    }

    @NotNull
    protected TimeSeriesDao getTimeSeriesDao(DSLContext dsl)
    {
        return new TimeSeriesDaoImpl(dsl);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = "office", required = true, description = "Specifies the owning office of the timeseries to be deleted.")
            },
            method = HttpMethod.DELETE,
            tags = {"TimeSeries"}
    )
    @Override
    public void delete(Context ctx, String tsId) {
        deleteRequests.mark();
        ((CwmsAuthorizer)ctx.appAttribute("Authorizer")).can_perform(ctx);

        String office = ctx.queryParam("office");

        try (
                final Timer.Context timeContext = deleteRequestsTime.time();
                DSLContext dsl = getDslContext(ctx))
        {
            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            dao.delete(office, tsId);
        }
        catch(DataAccessException ex)
        {
            RadarError re = new RadarError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

        ctx.status(HttpServletResponse.SC_OK);

    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="name", required=true, description="Specifies the name(s) of the time series whose data is to be included in the response. A case insensitive comparison is used to match names."),
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the time series(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   (default) Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="begin", required=false, description="Specifies the start of the time window for data to be included in the response. If this field is not specified, any required time window begins 24 hours prior to the specified or default end time. The format for this field is ISO 8601 extended, with optional offset and timezone, i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-0700[PST8PDT]'."),
            @OpenApiParam(name="end", required=false, description="Specifies the end of the time window for data to be included in the response. If this field is not specified, any required time window ends at the current time. The format for this field is ISO 8601 extended, with optional timezone, i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-0700[PST8PDT]'."),
            @OpenApiParam(name="timezone", required=false, description="Specifies the time zone of the values of the begin and end fields (unless otherwise specified), as well as the time zone of any times in the response. If this field is not specified, the default time zone of UTC shall be used.\r\nIgnored if begin was specified with offset and timezone."),
            @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)"),
            @OpenApiParam(name="page",
                          required = false,
                          description = "This end point can return a lot of data, this identifies where in the request you are. This is an opaque value, and can be obtained from the 'next-page' value in the response."
            ),
            @OpenApiParam(name="pageSize",
                          required=false,
                          type=Integer.class,
                          description = "How many entries per page returned. Default " + defaultPageSize + "."
            )
        },
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of elements of the data set you've selected.",
                                       content = {
                                           @OpenApiContent(from = TimeSeries.class, type=Formats.JSONV2),
                                           @OpenApiContent(from = TimeSeries.class, type=Formats.XML)
                                       }
                      ),
                      @OpenApiResponse(status="400", description = "Invalid parameter combination"),
                      @OpenApiResponse(status="404", description = "The provided combination of parameters did not find a timeseries."),
                      @OpenApiResponse(status="501",description = "Requested format is not implemented")
                    },
            method = HttpMethod.GET,
        tags = {"TimeSeries"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try (
                final Timer.Context timeContext = getAllRequestsTime.time();
                DSLContext dsl = getDslContext(ctx))
        {
            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            String format = ctx.queryParamAsClass("format", String.class).getOrDefault("");
            String names = ctx.queryParam("name");
            String office = ctx.queryParam("office");
            String unit = ctx.queryParamAsClass("unit", String.class).getOrDefault(UnitSystem.EN.getValue());
            String datum = ctx.queryParam("datum");
            String begin = ctx.queryParam("begin");
            String end = ctx.queryParam("end");
            String timezone = ctx.queryParamAsClass("timezone",String.class).getOrDefault("UTC");
            // The following parameters are only used for jsonv2 and xmlv2
            String cursor = ctx.queryParamAsClass("cursor", String.class).getOrDefault(
                    ctx.queryParamAsClass("page", String.class).getOrDefault(""));

            int pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(
                    ctx.queryParamAsClass("pagesize", Integer.class).getOrDefault(defaultPageSize));

            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, format);

            String results;
            String version = contentType.getParameters().get("version");

            ZoneId tz = ZoneId.of(timezone,ZoneId.SHORT_IDS);
            begin = begin != null ? begin : "PT-24H";

            ZonedDateTime beginZdt = DateUtils.parseUserDate(begin, timezone);
            ZonedDateTime endZdt = end != null
                                        ? DateUtils.parseUserDate(end, timezone)
                                        : ZonedDateTime.now(tz);

            if(version != null && version.equals("2"))
            {
                TimeSeries ts = dao.getTimeseries(cursor, pageSize, names, office, unit, datum, beginZdt, endZdt, tz);

                results = Formats.format(contentType, ts);
                ctx.status(HttpServletResponse.SC_OK);

                // Send back the link to the next page in the response header
                StringBuffer linkValue = new StringBuffer(600);
                linkValue.append(String.format("<%s>; rel=self; type=\"%s\"", buildRequestUrl(ctx, ts, ts.getPage()),
                        contentType));

                if(ts.getNextPage() != null)
                {
                    linkValue.append(",");
                    linkValue.append(
                            String.format("<%s>; rel=next; type=\"%s\"", buildRequestUrl(ctx, ts, ts.getNextPage()),
                                    contentType));
                }

                ctx.header("Link", linkValue.toString());
                ctx.result(results).contentType(contentType.toString());
            }
            else
            {
                if(format == null || format.isEmpty())
                {
                    format = "json";
                }


                results = dao.getTimeseries(format, names, office, unit, datum, beginZdt, endZdt, tz);
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);
            }
            requestResultSize.update(results.length());
        } catch (NotFoundException e){
            RadarError re = new RadarError("Not found.");
            logger.log(Level.WARNING, re.toString(), e);
            ctx.status(HttpServletResponse.SC_NOT_FOUND);
            ctx.json(re);
        } catch (IllegalArgumentException ex) {
            RadarError re = new RadarError("Invalid arguments supplied");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_BAD_REQUEST);
            ctx.json(re);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        getOneRequest.mark();
        try( final Timer.Context timeContext = getOneRequestTime.time() ){

            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    @OpenApi(
            description = "Update a TimeSeries",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                        @OpenApiContent(from = TimeSeries.class, type=Formats.XML)
                    },
                    required = true
            ),
            method = HttpMethod.PATCH,
            path = "/timeseries",
            tags = {"TimeSeries"}
    )
    @Override
    public void update(Context ctx, String id) {

        updateRequests.mark();
        ((CwmsAuthorizer)ctx.appAttribute("Authorizer")).can_perform(ctx);
        try (
                final Timer.Context timeContext = updateRequestsTime.time();
                DSLContext dsl = getDslContext(ctx))
        {
            TimeSeriesDao dao = getTimeSeriesDao(dsl);
            TimeSeries timeSeries = deserializeTimeSeries(ctx);

            dao.store(timeSeries, TimeSeriesDao.NON_VERSIONED);
            ctx.status(HttpServletResponse.SC_OK);
        }
        catch(IOException | DataAccessException ex)
        {
            RadarError re = new RadarError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private TimeSeries deserializeTimeSeries(Context ctx) throws IOException
    {
        return deserializeTimeSeries(ctx.body(), getContentType(ctx));
    }

    private TimeSeries deserializeTimeSeries(String body, ContentType contentType) throws IOException
    {
        return deserializeTimeSeries(body, contentType.getType());
    }

    public static TimeSeries deserializeTimeSeries(String body, String contentType) throws IOException
    {
        TimeSeries retval;

        if((Formats.XMLV2).equals(contentType))
        {
            // This is how it would be done if we could use jackson to parse the xml
            // it currently doesn't work because some of the jackson annotations
            // use certain naming conventions (e.g. "value-columns" vs "valueColumns")
            //  ObjectMapper om = buildXmlObjectMapper();
            //  retval = om.readValue(body, TimeSeries.class);
            retval = deserializeJaxb(body);
        } else if((Formats.JSONV2).equals(contentType)){
            ObjectMapper om = JsonV1.buildObjectMapper();
            retval = om.readValue(body, TimeSeries.class);
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    public static TimeSeries deserializeJaxb(String body) throws IOException
    {
        try
        {
            JAXBContext jaxbContext = JAXBContext.newInstance(TimeSeries.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return (TimeSeries) unmarshaller.unmarshal(new StringReader(body));
        }
        catch(JAXBException e)
        {
            throw new IOException(e);
        }
    }

    @NotNull
    public static ObjectMapper buildXmlObjectMapper()
    {
        ObjectMapper retval = new XmlMapper();
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        return retval;
    }

    @NotNull
    private ContentType getContentType(Context ctx)
    {
        String acceptHeader = ctx.req.getContentType();
        String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
        ContentType contentType = Formats.parseHeader(formatHeader);
        if(contentType == null)
        {
            throw new FormattingException("Format header could not be parsed");
        }
        return contentType;
    }

    /**
     * Builds a URL that references a specific "page" of the result.
     * @param ctx
     * @param ts
     * @return
     */
    private String buildRequestUrl(Context ctx, TimeSeries ts, String cursor)
    {
        StringBuffer result = ctx.req.getRequestURL();
        try {
            result.append(String.format("?name=%s", URLEncoder.encode(ts.getName(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&office=%s", URLEncoder.encode(ts.getOfficeId(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&unit=%s", URLEncoder.encode(ts.getUnits(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&begin=%s", URLEncoder.encode(ts.getBegin().format(DateTimeFormatter.ISO_ZONED_DATE_TIME), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&end=%s", URLEncoder.encode(ts.getEnd().format(DateTimeFormatter.ISO_ZONED_DATE_TIME), StandardCharsets.UTF_8.toString())));

            String format = ctx.queryParam("format");
            if(format != null && !format.isEmpty())
                result.append(String.format("&format=%s", format));

            if(cursor != null && !cursor.isEmpty())
                result.append(String.format("&page=%s", URLEncoder.encode(cursor, StandardCharsets.UTF_8.toString())));
        } catch (UnsupportedEncodingException ex) {
            // We shouldn't get here
            logger.log(Level.WARNING, null, ex);
        }
        return result.toString();
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = "office", description = "Specifies the owning office of the timeseries group(s) whose data is to be included in the response. If this field is not specified, matching timeseries groups information from all offices shall be returned."),
    },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {@OpenApiContent(isArray = true, from = Tsv.class, type = Formats.JSON)
                            }

                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of inputs provided the timeseries group(s) were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not implemented")
            },
            path = "/timeseries/recent",
            description = "Returns CWMS Timeseries Groups Data",
            tags = {"TimeSeries-Beta"},
            method = HttpMethod.GET
    )
    public void getRecent(Context ctx)
    {
        getRecentRequests.mark();
        try(final Timer.Context timeContext = getRecentRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            String office = ctx.queryParam("office");
            String categoryId = ctx.queryParamAsClass("category-id", String.class).allowNullable().get();
            String groupId = ctx.pathParamAsClass("group-id", String.class).allowNullable().get();
            String tsIdsParam = ctx.queryParamAsClass("ts-ids", String.class).allowNullable().get();

            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            gregorianCalendar.set(Calendar.HOUR, 0);
            gregorianCalendar.set(Calendar.MINUTE, 0);
            gregorianCalendar.set(Calendar.SECOND, 0);
            gregorianCalendar.set(Calendar.MILLISECOND, 0);

            gregorianCalendar.add(Calendar.HOUR, 24 * 14);
            Timestamp futureLimit = Timestamp.from(gregorianCalendar.toInstant());
            gregorianCalendar.add(Calendar.HOUR, 24 * -28);
            Timestamp pastLimit = Timestamp.from(gregorianCalendar.toInstant());

            boolean hasTsGroupInfo = categoryId != null && !categoryId.isEmpty() && groupId != null && !groupId.isEmpty();
            List<String> tsIds = getTsIds(tsIdsParam);
            boolean hasTsIds = tsIds != null && !tsIds.isEmpty();

            List<RecentValue> latestValues = null;
            if(hasTsGroupInfo && hasTsIds){
                // has both = this is an error
                RadarError re = new RadarError("Invalid arguments supplied, group has both Timeseries Group info and Timeseries IDs.");
                logger.log(Level.SEVERE, re.toString() + " for url " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if(!hasTsGroupInfo && !hasTsIds){
                // doesn't have either?  Just return empty results?
                RadarError re = new RadarError("Invalid arguments supplied, group has neither Timeseries Group info nor Timeseries IDs");
                logger.log(Level.SEVERE, re.toString()+ " for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if( hasTsGroupInfo){
                // just group provided
                latestValues = dao.findRecentsInRange(office, categoryId, groupId, pastLimit, futureLimit);
            } else if(hasTsIds){
                latestValues = dao.findMostRecentsInRange(tsIds, pastLimit, futureLimit);
            }

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "json");

            String result = Formats.format(contentType, latestValues,RecentValue.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    public static List<String> getTsIds(String tsIdsParam)
    {
        List<String> retval = null;

        if(tsIdsParam != null && !tsIdsParam.isEmpty()){
            retval = new ArrayList<>();

            if(tsIdsParam.startsWith("[")){
                tsIdsParam = tsIdsParam.substring(1);
            }

            if(tsIdsParam.endsWith("]")){
                tsIdsParam = tsIdsParam.substring(0, tsIdsParam.length() -1);
            }

            if(!tsIdsParam.isEmpty())
            {
                final String regex = "\"[^\"]*\"|[^,]+";
                final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

                try(Scanner s = new Scanner(tsIdsParam)) {
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
                1000, Spliterator.ORDERED|Spliterator.NONNULL) {
            public boolean tryAdvance(Consumer<? super MatchResult> action) {
                if(s.findWithinHorizon(pattern, 0)!=null) {
                    action.accept(s.match());
                    return true;
                }
                else return false;
            }
        }, false);
    }
}
