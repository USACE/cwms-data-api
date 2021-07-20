package cwms.radar.api;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.LocationsDao;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.geojson.FeatureCollection;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;


/**
 *
 *
 */
public class LocationController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(LocationController.class.getName());
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public LocationController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam( name="names", description = "Specifies the name(s) of the location(s) whose data is to be included in the response"),
            @OpenApiParam(name="office", description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit",   description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum",  description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="format", description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)\n" + "6.    geojson")
        },
        responses = {
            @OpenApiResponse( status="200",
                    content = {
                            @OpenApiContent(type = Formats.JSON ),
                            @OpenApiContent(type = Formats.TAB ),
                            @OpenApiContent(type = Formats.CSV ),
                            @OpenApiContent(type = Formats.XML ),
                            @OpenApiContent(type = Formats.WML2),
                            @OpenApiContent(type = Formats.GEOJSON )
                    })
        },
        description = "Returns CWMS Location Data",
        tags = {"Locations"}
    )
    @Override
    public void getAll(Context ctx)
    {
        getAllRequests.mark();
        try(final Timer.Context timeContext = getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationsDao cdm = new LocationsDao(dsl);

            String names = ctx.queryParam("names");
            String units = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String office = ctx.queryParam("office");

            String formatParm = ctx.queryParam("format", "");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
            ctx.contentType(contentType.toString());

            final String results;
            if(contentType.getType().equals(Formats.GEOJSON))
            {
                logger.info("units:" + units);
                FeatureCollection collection = cdm.buildFeatureCollection(names, units, office);
                ObjectMapper mapper = JavalinJackson.getObjectMapper();
                results = mapper.writeValueAsString(collection);
            }
            else
            {
                String format = getFormatFromContent(contentType);
                results = cdm.getLocations(names, format, units, datum, office);
            }

            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        }
        catch( JsonProcessingException ex)
        {
            RadarError re = new RadarError("failed to process request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
        catch(FormattingException fe)
        {

            if(fe.getCause() instanceof IOException)
            {
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ctx.result("server error");
            }
            else
            {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Invalid Format Options");
            }
            logger.log(Level.SEVERE, "failed to format data", fe);
        }
    }

    private String getFormatFromContent(ContentType contentType)
    {
        String format = "json";
        if(contentType != null)
        {
            String type = contentType.getType();
            // Seems weird to map back to format from contentType but we really want them to agree.
            // What if format wasn't provided but an accept header for csv was?
            // I think we would want to pass "csv" to the db procedure.
            Map<String, String> lookup = new LinkedHashMap<>();
            lookup.put(Formats.TAB, "tab");
            lookup.put(Formats.CSV, "csv");
            lookup.put(Formats.XML, "xml");
            lookup.put(Formats.WML2, "wml2");
            lookup.put(Formats.JSON, "json");
            if(lookup.containsKey(type))
            {
                format = lookup.get(type);
            }
        }
        return format;
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String locationCode) {
        getOneRequest.mark();
        try (
            final Timer.Context timeContext = getOneRequestTime.time()
            ){
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
