package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.cda.api.Controllers.ANY_MASK;
import static cwms.cda.api.Controllers.BOTTOM_MASK;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.INCLUDE_EXPLICIT;
import static cwms.cda.api.Controllers.INCLUDE_IMPLICIT;
import static cwms.cda.api.Controllers.NAME_MASK;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGESIZE2;
import static cwms.cda.api.Controllers.PAGESIZE3;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.POOL_ID;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.TOP_MASK;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.PoolDao;
import cwms.cda.data.dto.Pool;
import cwms.cda.data.dto.Pools;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
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
    private static final int defaultPageSize = 100;

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public PoolController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
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
            @OpenApiParam(name = PAGESIZE3,
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
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
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

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(PoolController.class.getName(),
                            GET_ALL));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE, PAGESIZE3,
                PAGESIZE2}, Integer.class, defaultPageSize, metrics,
                    name(PoolController.class.getName(), GET_ALL));

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
        try (final Timer.Context timeContext = markAndTime(GET_ONE);
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
                CdaError re = new CdaError("Unable to find pool based on parameters given");
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
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }
}
