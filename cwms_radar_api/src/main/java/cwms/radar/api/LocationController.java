package cwms.radar.api;

import java.io.IOException;
import java.sql.SQLException;
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
import cwms.radar.data.CwmsDataManager;
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

import static com.codahale.metrics.MetricRegistry.name;


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
            @OpenApiParam( name="names",required = false, description = "Specifies the name(s) of the location(s) whose data is to be included in the response"),
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)\n" + "6.    geojson")
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
                    }),
            @OpenApiResponse( status="404", description = "Based on the combination of inputs provided the location(s) were not found."),
            @OpenApiResponse( status="501", description = "request format is not implemented")
        },
        description = "Returns CWMS Location Data",
        tags = {"Locations"}
    )
    @Override
    public void getAll(Context ctx)
    {
        getAllRequests.mark();
        try(final Timer.Context time_context = getAllRequestsTime.time();
            CwmsDataManager cdm = new CwmsDataManager(ctx);)
        {
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
        catch(SQLException | JsonProcessingException ex)
        {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
        catch(FormattingException fe)
        {
            logger.log(Level.SEVERE, "failed to format data", fe);
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
    public void getOne(Context ctx, String location_code) {
        getOneRequest.mark();
        try (
            final Timer.Context time_context = getOneRequestTime.time();
            ){
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String location_code) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String location_code) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
