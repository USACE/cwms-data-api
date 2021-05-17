package cwms.radar.api;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.*;
import com.codahale.metrics.Timer;

import cwms.radar.data.CwmsDataManager;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

public class UnitsController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());
    private final MetricRegistry metrics;// = new MetricRegistry();
    private final Meter getAllRequests;// = metrics.meter(OfficeController.class.getName()+"."+"getAll.count");
    private final Timer getAllRequestsTime;// =metrics.timer(OfficeController.class.getName()+"."+"getAll.time");
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public UnitsController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String unit) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="format",required = false, description = "Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)")
        },
        responses = {
            @OpenApiResponse( status = "200"),
            @OpenApiResponse(status="501",description = "The format requested is not implemented")
        },
        tags = { "Units"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try (
            final Timer.Context time_context = getAllRequestsTime.time();
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            String format = ctx.queryParam("format","json");


            switch(format){
                case "json": {ctx.contentType(Formats.JSON); break;}
                case "tab": {ctx.contentType(Formats.TAB); break;}
                case "csv": {ctx.contentType(Formats.CSV); break;}
                case "xml": {ctx.contentType(Formats.XML); break;}
                case "wml2": {ctx.contentType(Formats.WML2); break;}
                default: throw new UnsupportedOperationException("Format " +  format + " is not implemented for this end point");
            }

            String results = cdm.getUnits(format);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }

    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String unit) {
        getOneRequest.mark();
        try (
            final Timer.Context time_context = getOneRequestTime.time();
            ){
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String unit) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

}
