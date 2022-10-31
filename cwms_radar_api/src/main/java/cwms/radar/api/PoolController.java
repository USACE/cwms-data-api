package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.PoolDao;
import cwms.radar.data.dto.Pool;
import cwms.radar.data.dto.Pools;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class PoolController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(PoolController.class.getName());
    public static final String ANY_MASK = "*";
    private static final int defaultPageSize = 100;
    public static final String OFFICE = "office";
    public static final String ID_MASK = "id-mask";
    public static final String NAME_MASK = "name-mask";
    public static final String BOTTOM_MASK = "bottom-mask";
    public static final String TOP_MASK = "top-mask";
    public static final String INCLUDE_EXPLICIT = "include-explicit";
    public static final String INCLUDE_IMPLICIT = "include-implicit";
    public static final String PAGE = "page";
    public static final String CURSOR = "cursor";
    public static final String PAGE_SIZE = "page-size";
    public static final String PAGE_SIZE_CAMEL = "pageSize";
    public static final String POOL_ID = "pool-id";
    public static final String PROJECT_ID = "project-id";

    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public PoolController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className, "getAll", "count"));
        getAllRequestsTime = this.metrics.timer(name(className, "getAll", "time"));
        getOneRequest = this.metrics.meter(name(className, "getOne", "count"));
        getOneRequestTime = this.metrics.timer(name(className, "getOne", "time"));
        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the data"
                    + " in the response. If this field is not specified, matching items from all"
                    + " offices shall be returned."),
            @OpenApiParam(name = ID_MASK, description = "Project Id mask. Default value:*"),
            @OpenApiParam(name = NAME_MASK, description = "Name mask. Default value:*"),
            @OpenApiParam(name = BOTTOM_MASK, description = "Bottom level mask. Default"
                    + " value:*"),
            @OpenApiParam(name = TOP_MASK, description = "Top level mask. Default value:*"),
            @OpenApiParam(name = INCLUDE_EXPLICIT, description = "Specifies if the results"
                    + " should include explicit Pools. Default value:false"),
            @OpenApiParam(name = INCLUDE_IMPLICIT, description = "Specifies if the results"
                    + " should include implicit Pools. Default value:true"),
            @OpenApiParam(name = PAGE,
                    description = "This end point can return a lot of data, this identifies where"
                            + " in the request you are. This is an opaque value, and can be"
                            + " obtained from the 'next-page' value in the response."
            ),
            @OpenApiParam(name = CURSOR,
                    deprecated = true,
                    description = "Deprecated. Use '" + PAGE + "' instead."
            ),
            @OpenApiParam(name = PAGE_SIZE,
                    type = Integer.class,
                    description =
                            "How many entries per page returned. Default " + defaultPageSize + "."
            ),
            @OpenApiParam(name = PAGE_SIZE_CAMEL,
                    deprecated = true,
                    type = Integer.class,
                    description = "Deprecated. Use '" + PAGE_SIZE + "' instead."),},
            responses = {
                    @OpenApiResponse(status = "200", content = {
                            @OpenApiContent(type = Formats.JSONV2, from = Pools.class)}),
                    @OpenApiResponse(status = "404", description = "Based on the combination of"
                            + " inputs provided the pools were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not"
                            + " implemented")},
            description = "Returns Pools Data",
            tags = {"Pools"})
    @Override
    public void getAll(@NotNull Context ctx) {
        getAllRequests.mark();
        try (final Timer.Context timeContext = getAllRequestsTime.time();
             DSLContext dsl = getDslContext(ctx)) {
            PoolDao dao = new PoolDao(dsl);
            String office = ctx.queryParam(OFFICE);

            String projectIdMask =
                    ctx.queryParamAsClass(ID_MASK, String.class).getOrDefault(ANY_MASK);
            String nameMask =
                    ctx.queryParamAsClass(NAME_MASK, String.class).getOrDefault(ANY_MASK);
            String bottomMask =
                    ctx.queryParamAsClass(BOTTOM_MASK, String.class).getOrDefault(ANY_MASK);
            String topMask = ctx.queryParamAsClass(TOP_MASK, String.class).getOrDefault(ANY_MASK);

            String isExp = ctx.queryParamAsClass(INCLUDE_EXPLICIT, String.class).getOrDefault(
                    "false");
            boolean isExplicit = Boolean.parseBoolean(isExp);
            String isImp = ctx.queryParamAsClass(INCLUDE_IMPLICIT, String.class)
                    .getOrDefault("true");
            boolean isImplicit = Boolean.parseBoolean(isImp);

            String cursor = Controllers.queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(PoolController.class.getName(),
                            "getAll"));

            int pageSize = Controllers.queryParamAsClass(ctx, new String[]{PAGE_SIZE, PAGE_SIZE_CAMEL,
                "pagesize"}, Integer.class, defaultPageSize, metrics,
                    name(PoolController.class.getName(), "getAll"));

            Pools pools = dao.retrievePools(cursor, pageSize, projectIdMask, nameMask, bottomMask,
                    topMask, isExplicit, isImplicit, office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

            String result = Formats.format(contentType, pools);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = POOL_ID, required = true, description = "Specifies the"
                            + " pool whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the"
                            + " owning office of the Pool whose data is to be included in the"
                            + " response."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies"
                            + " the project-id of the Pool whose data is to be included in the"
                            + " response."),
                    @OpenApiParam(name = BOTTOM_MASK, description = "Bottom level mask. Default"
                            + " value:*"),
                    @OpenApiParam(name = TOP_MASK, description = "Top level mask. Default"
                            + " value:*"),
                    @OpenApiParam(name = INCLUDE_EXPLICIT, description = "Specifies if the"
                            + " results should include explicit Pools. Default value:false"),
                    @OpenApiParam(name = INCLUDE_IMPLICIT, description = "Specifies if the"
                            + " results should include implicit Pools. Default value:true"),

            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = Pool.class, type = Formats.JSONV2)
                            }
                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the Location Category was not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested Pool", tags = {"Pools"})
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String poolId) {
        getOneRequest.mark();
        try (final Timer.Context timeContext = getOneRequestTime.time();
             DSLContext dsl = getDslContext(ctx)) {
            PoolDao dao = new PoolDao(dsl);

            // These are required
            String office = ctx.queryParam(OFFICE);
            String projectId = ctx.queryParam(PROJECT_ID);

            // These are optional
            String bottomMask =
                    ctx.queryParamAsClass(BOTTOM_MASK, String.class).getOrDefault(ANY_MASK);
            String topMask = ctx.queryParamAsClass(TOP_MASK, String.class).getOrDefault(ANY_MASK);
            String isExp = ctx.queryParamAsClass(INCLUDE_EXPLICIT, String.class).getOrDefault(
                    "true");
            boolean isExplicit = Boolean.parseBoolean(isExp);
            String isImp = ctx.queryParamAsClass(INCLUDE_IMPLICIT, String.class).getOrDefault(
                    "true");
            boolean isImplicit = Boolean.parseBoolean(isImp);

            // I want to call retrievePool but it doesn't return implicit pools
            // pool = dao.retrievePool(projectId, poolId, office);
            Pool pool = dao.retrievePoolFromCatalog(projectId, poolId, bottomMask, topMask,
                    isExplicit, isImplicit, office);

            if (pool == null) {
                RadarError re = new RadarError("Unable to find pool based on parameters given");
                logger.info(() -> {
                    String fullUrl = ctx.fullUrl();
                    return re + System.lineSeparator() + "for request " + fullUrl;
                });
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
                ctx.contentType(contentType.toString());

                String result = Formats.format(contentType, pool);

                ctx.result(result);
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            }
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String locationCode) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String locationCode) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
