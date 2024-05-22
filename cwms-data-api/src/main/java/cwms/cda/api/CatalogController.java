package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.BOUNDING_OFFICE_LIKE;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATIONS;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMESERIES;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
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
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

public class CatalogController implements CrudHandler {

    private static final Logger logger = Logger.getLogger(CatalogController.class.getName());
    private static final String TAG = "Catalog";


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
    public void delete(Context ctx, @NotNull String entry) {
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

                @OpenApiParam(name = PAGE_SIZE,
                        type = Integer.class,
                        description = "How many entries per page returned. Default 500."
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
                        description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the id"
                ),
                @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE,
                        description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the "
                                + "timeseries category id"
                ),
                @OpenApiParam(name = TIMESERIES_GROUP_LIKE,
                        description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the "
                                + "timeseries group id"
                ),
                @OpenApiParam(name = LOCATION_CATEGORY_LIKE,
                        description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the location"
                                + " category id"
                ),
                @OpenApiParam(name = LOCATION_GROUP_LIKE,
                        description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the location"
                                + " group id"
                ),
                @OpenApiParam(name = BOUNDING_OFFICE_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                        + "matching against the location bounding office. "
                        + "When this field is used items with no bounding office set will not be present in results."),
                @OpenApiParam(name = Controllers.INCLUDE_EXTENTS, type = Boolean.class, description = "Whether the returned "
                        + "catalog entries should include timeseries extents. Only valid for TIMESERIES. "
                        + "Default is true."),
                @OpenApiParam(name = Controllers.EXCLUDE_EMPTY_EXTENTS, type = Boolean.class, description = "Specifies "
                        + "whether Timeseries that have only empty extents [null, null, null, null] "
                        + "should be excluded from the results.  This does not control whether the "
                        + "extents are returned to the user, only whether matching timeseries are "
                        + "excluded. Only valid for TIMESERIES. Default is true."),
            },
            pathParams = {
                @OpenApiParam(name = "dataset",
                        type = CatalogableEndpoint.class,
                        description = "A list of what data? E.g. Timeseries, Locations, Ratings, etc")
            },
            responses = {@OpenApiResponse(status = STATUS_200,
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
    public void getOne(@NotNull Context ctx, @NotNull String dataSet) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = JooqDao.getDslContext(ctx);

            String valDataSet =
                    ((PolicyFactory) ctx.appAttribute("PolicyFactory")).sanitize(dataSet);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(CatalogController.class.getName(), GET_ONE));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE              },
                    Integer.class, defaultPageSize, metrics,
                    name(CatalogController.class.getName(), GET_ONE));

            String unitSystem = queryParamAsClass(ctx,
                    new String[]{UNIT_SYSTEM, },
                    String.class, UnitSystem.SI.getValue(), metrics,
                    name(CatalogController.class.getName(), GET_ONE));

            String office = ctx.queryParamAsClass(OFFICE, String.class).allowNullable()
                            .check(Office::validOfficeCanNull, "Invalid office provided")
                            .get();

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String tsCategoryLike = queryParamAsClass(ctx, new String[]{TIMESERIES_CATEGORY_LIKE},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String tsGroupLike = queryParamAsClass(ctx, new String[]{TIMESERIES_GROUP_LIKE},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locCategoryLike = queryParamAsClass(ctx, new String[]{LOCATION_CATEGORY_LIKE},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locGroupLike = queryParamAsClass(ctx, new String[]{LOCATION_GROUP_LIKE },
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String boundingOfficeLike = queryParamAsClass(ctx, new String[]{BOUNDING_OFFICE_LIKE},
                    String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String acceptHeader = ctx.header(ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, null);
            Catalog cat = null;
            if (TIMESERIES.equalsIgnoreCase(valDataSet)) {
                TimeSeriesDao tsDao = new TimeSeriesDaoImpl(dsl, metrics);

                boolean includeExtents = ctx.queryParamAsClass(Controllers.INCLUDE_EXTENTS, Boolean.class).getOrDefault(true);
                boolean excludeExtents = ctx.queryParamAsClass(Controllers.EXCLUDE_EMPTY_EXTENTS, Boolean.class).getOrDefault(true);

                TimeSeriesDaoImpl.CatalogRequestParameters parameters = new TimeSeriesDaoImpl.CatalogRequestParameters.Builder()
                        .withOffice(office)
                        .withIdLike(like)
                        .withLocCatLike(locCategoryLike)
                        .withLocGroupLike(locGroupLike)
                        .withTsCatLike(tsCategoryLike)
                        .withTsGroupLike(tsGroupLike)
                        .withBoundingOfficeLike(boundingOfficeLike)
                        .withIncludeExtents(includeExtents)
                        .withExcludeEmptyExtents(excludeExtents)
                        .build();

                cat = tsDao.getTimeSeriesCatalog(cursor, pageSize, parameters);

            } else if (LOCATIONS.equalsIgnoreCase(valDataSet)) {
                LocationsDao dao = new LocationsDaoImpl(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, unitSystem, office, like,
                        locCategoryLike, locGroupLike, boundingOfficeLike);
            }
            if (cat != null) {
                String data = Formats.format(contentType, cat);
                ctx.result(data).contentType(contentType.toString());
                requestResultSize.update(data.length());
            } else {
                final CdaError re = new CdaError("Cannot create catalog of requested "
                        + "information");

                logger.info(() -> re + "with url:" + ctx.fullUrl());
                ctx.json(re).status(HttpCode.NOT_FOUND);
            }
        }
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void update(Context ctx, @NotNull String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
