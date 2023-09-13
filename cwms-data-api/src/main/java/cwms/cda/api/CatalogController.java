package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATIONS;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE2;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE2;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGESIZE2;
import static cwms.cda.api.Controllers.PAGESIZE3;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.TIMESERIES;
import static cwms.cda.api.Controllers.TIMESERIESCATEGORYLIKE2;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE2;
import static cwms.cda.api.Controllers.UNITSYSTEM2;
import static cwms.cda.api.Controllers.UNIT_SYSTEM;
import static cwms.cda.api.Controllers.queryParamAsClass;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.logging.Logger;
import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

public class CatalogController implements CrudHandler {

    private static final Logger logger = Logger.getLogger(CatalogController.class.getName());
    private static final String TAG = "Catalog-Beta";


    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    private final int defaultPageSize = 500;

    public CatalogController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(tags = {TAG}, ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void delete(Context ctx, String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void getAll(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are."
                    ),
                    @OpenApiParam(name = CURSOR,
                            deprecated = true,
                            description = "Deprecated. Use 'page' instead."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entires per page returned. Default 500."
                    ),
                    @OpenApiParam(name = PAGESIZE3,
                            deprecated = true,
                            type = Integer.class,
                            description = "Deprecated. Use page-size."
                    ),
                    @OpenApiParam(name = UNITSYSTEM2,
                            deprecated = true,
                            type = UnitSystem.class,
                            description = "Deprecated. Use unit-system."
                    ),
                    @OpenApiParam(name = UNIT_SYSTEM,
                            type = UnitSystem.class,
                            description = UnitSystem.DESCRIPTION
                    ),
                    @OpenApiParam(name = OFFICE,
                            description = "3-4 letter office name representing the district you "
                                    + "want to isolate data to."
                    ),
                    @OpenApiParam(name = LIKE,
                            description = "Posix regular expression matching against the id"
                    ),
                    @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE,
                            description = "Posix regular expression matching against the "
                                    + "timeseries category id"
                    ),
                    @OpenApiParam(name = TIMESERIESCATEGORYLIKE2,
                            deprecated = true,
                            description = "Deprecated. Use timeseries-category-like."
                    ),
                    @OpenApiParam(name = TIMESERIES_GROUP_LIKE,
                            description = "Posix regular expression matching against the "
                                    + "timeseries group id"
                    ),
                    @OpenApiParam(name = TIMESERIES_GROUP_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use timeseries-group-like."
                    ),
                    @OpenApiParam(name = LOCATION_CATEGORY_LIKE,
                            description = "Posix regular expression matching against the location"
                                    + " category id"
                    ),
                    @OpenApiParam(name = LOCATION_CATEGORY_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use location-category-like."
                    ),
                    @OpenApiParam(name = LOCATION_GROUP_LIKE,
                            description = "Posix regular expression matching against the location"
                                    + " group id"
                    ),
                    @OpenApiParam(name = LOCATION_GROUP_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use location-group-like."
                    )
            },
            pathParams = {
                    @OpenApiParam(name = "dataset",
                            type = CatalogableEndpoint.class,
                            description = "A list of what data? E.g. Timeseries, Locations, "
                                    + "Ratings, etc")
            },
            responses = {@OpenApiResponse(status = "200",
                    description = "A list of elements the data set you've selected.",
                    content = {
                            @OpenApiContent(from = Catalog.class, type = Formats.JSONV2),
                            @OpenApiContent(from = Catalog.class, type = Formats.XML)
                    }
            )
            },
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String dataSet) {

        try (
                final Timer.Context timeContext = markAndTime(GET_ONE);
                DSLContext dsl = JooqDao.getDslContext(ctx)
        ) {

            String valDataSet =
                    ((PolicyFactory) ctx.appAttribute("PolicyFactory")).sanitize(dataSet);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(CatalogController.class.getName(), GET_ONE));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE, PAGESIZE3,
                    PAGESIZE2}, Integer.class, defaultPageSize, metrics,
                    name(CatalogController.class.getName(), GET_ONE));

            String unitSystem = queryParamAsClass(ctx,
                    new String[]{UNIT_SYSTEM, UNITSYSTEM2},
                    String.class, UnitSystem.SI.getValue(), metrics,
                    name(CatalogController.class.getName(), GET_ONE));

            String office = ctx.queryParamAsClass(OFFICE, String.class).allowNullable()
                            .check(Office::validOfficeCanNull, "Invalid office provided")
                            .get();

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String tsCategoryLike = queryParamAsClass(ctx, new String[]{TIMESERIES_CATEGORY_LIKE, TIMESERIESCATEGORYLIKE2},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String tsGroupLike = queryParamAsClass(ctx, new String[]{TIMESERIES_GROUP_LIKE, TIMESERIES_GROUP_LIKE2},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locCategoryLike = queryParamAsClass(ctx, new String[]{LOCATION_CATEGORY_LIKE, LOCATION_CATEGORY_LIKE2},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locGroupLike = queryParamAsClass(ctx, new String[]{LOCATION_GROUP_LIKE, LOCATION_GROUP_LIKE2},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String acceptHeader = ctx.header(ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, null);
            Catalog cat = null;
            if (TIMESERIES.equalsIgnoreCase(valDataSet)) {
                TimeSeriesDao tsDao = new TimeSeriesDaoImpl(dsl);
                cat = tsDao.getTimeSeriesCatalog(cursor, pageSize, office, like, locCategoryLike,
                        locGroupLike, tsCategoryLike, tsGroupLike);
            } else if (LOCATIONS.equalsIgnoreCase(valDataSet)) {
                LocationsDao dao = new LocationsDaoImpl(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, unitSystem, office, like,
                        locCategoryLike, locGroupLike);
            }
            if (cat != null) {
                String data = Formats.format(contentType, cat);
                ctx.result(data).contentType(contentType.toString());
                requestResultSize.update(data.length());
            } else {
                final CdaError re = new CdaError("Cannot create catalog of requested "
                        + "information");

                logger.info(() -> re.toString() + "with url:" + ctx.fullUrl());
                ctx.json(re).status(HttpCode.NOT_FOUND);
            }
        }
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void update(Context ctx, String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
