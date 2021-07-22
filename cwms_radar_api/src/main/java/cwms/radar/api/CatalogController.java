package cwms.radar.api;

import java.sql.SQLException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.*;

import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.CwmsDataManager;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.LocationsDao;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;

import static com.codahale.metrics.MetricRegistry.*;

import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.*;

public class CatalogController implements CrudHandler{

    private static final Logger logger = Logger.getLogger(CatalogController.class.getName());


    private final MetricRegistry metrics;// = new MetricRegistry();
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public CatalogController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void delete(Context ctx, String entry) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void getAll(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="cursor",
                          required = false,
                          description = "This end point can return a lot of data, this identifies where in the request you are."
            ),
            @OpenApiParam(name="pageSize",
                          required= false,
                          type=Integer.class,
                          description = "How many entires per page returned. Default 500."
            ),
            @OpenApiParam(name="unitSystem",
                          required = false,
                          type = UnitSystem.class,
                          description = UnitSystem.DESCRIPTION
            ),
            @OpenApiParam(name="office",
                          required = false,
                          description = "3-4 letter office name representing the district you want to isolate data to."
            )
        },
        pathParams = {
            @OpenApiParam(name="dataSet",
                          required = false,
                          type = CatalogableEndpoint.class,
                          description = "A list of what data? E.g. Timeseries, Locations, Ratings, etc")
        },
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of elements the data set you've selected.",
                                       content = {
                                           @OpenApiContent(from = Catalog.class, type=Formats.JSONV2),
                                           @OpenApiContent(from = Catalog.class, type=Formats.XML)
                                       }
                      )
                    },
        tags = {"Catalog"}
    )
    @Override
    public void getOne(Context ctx, String dataSet) {
        getOneRequest.mark();
        try (
            final Timer.Context time_context = getOneRequestTime.time();
            CwmsDataManager cdm = new CwmsDataManager(ctx);
            DSLContext dsl = JooqDao.getDslContext(ctx);
        ) {
            String valDataSet = ctx.appAttribute(PolicyFactory.class).sanitize(dataSet);
            String cursor = ctx.queryParam("cursor",String.class,"").getValue();
            logger.info("getting pageSize");
            int pageSize = ctx.queryParam("pageSize",Integer.class,"500").getValue().intValue();
            logger.info("This is a test");
            String unitSystem = ctx.queryParam("unitSystem",UnitSystem.class,"SI").getValue().getValue();
            Optional<String> office = Optional.ofNullable(
                                         ctx.queryParam("office", String.class, null)
                                            .check( ofc -> Office.validOfficeCanNull(ofc)
                                           ).getOrNull());
            String acceptHeader = ctx.header("Accept");
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, null);
            Catalog cat = null;
            if( "timeseries".equalsIgnoreCase(valDataSet)){
                cat = cdm.getTimeSeriesCatalog(cursor, pageSize, office );
            } else if ("locations".equalsIgnoreCase(valDataSet)){
                LocationsDao dao = new LocationsDao(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, unitSystem, office );
                //cat = cdm.getLocationCatalog(cursor, pageSize, unitSystem, office );
            }
            if( cat != null ){
                String data = Formats.format(contentType, cat);
                ctx.result(data).contentType(contentType.toString());
                requestResultSize.update(data.length());
            } else {
                final RadarError re = new RadarError("Cannot create catalog of requested information");

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                        .append("with url:" ).append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
            }

        } catch( SQLException er) {
            RadarError re = new RadarError("failed to process request");
            logger.log(Level.SEVERE, re.toString(), er);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void update(Context ctx, String entry) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
