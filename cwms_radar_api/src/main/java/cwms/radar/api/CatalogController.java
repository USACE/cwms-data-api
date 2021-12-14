package cwms.radar.api;

import java.util.Optional;
import java.util.logging.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.LocationsDao;
import cwms.radar.data.dao.LocationsDaoImpl;
import cwms.radar.data.dao.TimeSeriesDao;
import cwms.radar.data.dao.TimeSeriesDaoImpl;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

import static com.codahale.metrics.MetricRegistry.name;

public class CatalogController implements CrudHandler{

    private static final Logger logger = Logger.getLogger(CatalogController.class.getName());
    private static final String TAG = "Catalog-Beta";

    private final MetricRegistry metrics;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    private final int defaultPageSize = 500;

    public CatalogController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(tags = {TAG},ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void delete(Context ctx, String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void getAll(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="page",
                          description = "This end point can return a lot of data, this identifies where in the request you are."
            ),
            @OpenApiParam(name="pageSize",
                          type=Integer.class,
                          description = "How many entires per page returned. Default 500."
            ),
            @OpenApiParam(name="unitSystem",
                          type = UnitSystem.class,
                          description = UnitSystem.DESCRIPTION
            ),
            @OpenApiParam(name="office",
                          description = "3-4 letter office name representing the district you want to isolate data to."
            ),
            @OpenApiParam(name="like",
                    description = "Posix regular expression matching against the id"
            ),
            @OpenApiParam(name="timeseriesCategoryLike",
                    description = "Posix regular expression matching against the timeseries category id"
            ),
            @OpenApiParam(name="timeseriesGroupLike",
                    description = "Posix regular expression matching against the timeseries group id"
            ),
            @OpenApiParam(name="locationCategoryLike",
                    description = "Posix regular expression matching against the location category id"
            ),
            @OpenApiParam(name="locationGroupLike",
                    description = "Posix regular expression matching against the location group id"
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
        tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String dataSet) {
        getOneRequest.mark();
        try (
            final Timer.Context timeContext = getOneRequestTime.time();
            DSLContext dsl = JooqDao.getDslContext(ctx)
        ) {

            String valDataSet = ((PolicyFactory) ctx.appAttribute("PolicyFactory")).sanitize(dataSet);
            String cursor = ctx.queryParamAsClass("cursor",String.class)
                               .getOrDefault(ctx.queryParamAsClass("page", String.class).getOrDefault(""));
            int pageSize = ctx.queryParamAsClass("pageSize",Integer.class)
								.getOrDefault(
									ctx.queryParamAsClass("pagesize",Integer.class).getOrDefault(defaultPageSize)
								);
            String unitSystem = ctx.queryParamAsClass("unitSystem",String.class).getOrDefault(UnitSystem.SI.getValue());
            Optional<String> office = Optional.ofNullable(
                                         ctx.queryParamAsClass("office", String.class).allowNullable()
                                            .check(Office::validOfficeCanNull, "Invalid office provided" )
                                            .get()
                                        );

            String like = ctx.queryParamAsClass("like",String.class).getOrDefault(".*");
            String tsCategoryLike = ctx.queryParamAsClass("timeseriesCategoryLike",String.class).getOrDefault(null);
            String tsGroupLike = ctx.queryParamAsClass("timeseriesGroupLike",String.class).getOrDefault(null);
            String locCategoryLike = ctx.queryParamAsClass("locationCategoryLike",String.class).getOrDefault(null);
            String locGroupLike = ctx.queryParamAsClass("locationGroupLike",String.class).getOrDefault(null);

            String acceptHeader = ctx.header("Accept");
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, null);
            Catalog cat = null;
            if( "timeseries".equalsIgnoreCase(valDataSet)){
                TimeSeriesDao tsDao = new TimeSeriesDaoImpl(dsl);
                cat = tsDao.getTimeSeriesCatalog(cursor, pageSize, office, like, locCategoryLike, locGroupLike,tsCategoryLike, tsGroupLike);
            } else if ("locations".equalsIgnoreCase(valDataSet)){
                LocationsDao dao = new LocationsDaoImpl(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, unitSystem, office, like, locCategoryLike, locGroupLike );
            }
            if( cat != null ){
                String data = Formats.format(contentType, cat);
                ctx.result(data).contentType(contentType.toString());
                requestResultSize.update(data.length());
            } else {
                final RadarError re = new RadarError("Cannot create catalog of requested information");

                logger.info(() -> re.toString() + "with url:" + ctx.fullUrl());
                ctx.json(re).status(HttpCode.NOT_FOUND);
            }
        }
    }

    @OpenApi(tags = {"Catalog"},ignore = true)
    @Override
    public void update(Context ctx, String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
