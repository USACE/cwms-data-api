package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.PoolDao;
import cwms.cda.data.dto.Pool;
import cwms.cda.data.dto.Pools;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class PoolController implements CrudHandler {

    static final String TAG = "Pools";

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
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
            @OpenApiParam(name = CURSOR, deprecated = true,
                    description = "This end point can return a lot of data, this "
                            + "identifies where in the request you are. This is an opaque"
                            + " value, and can be obtained from the 'next-page' value in "
                            + "the response. Deprecated, use " + PAGE + " instead."),
            @OpenApiParam(name = PAGE_SIZE,
                    type = Integer.class,
                    description =
                            "How many entries per page returned. Default " + defaultPageSize + "."
            ),},
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(type = Formats.JSONV2, from = Pools.class)}),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of"
                            + " inputs provided the pools were not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not"
                            + " implemented")},
            description = "Returns Pools Data",
            tags = {TAG})
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

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

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                    Integer.class, defaultPageSize, metrics,
                    name(PoolController.class.getName(), GET_ALL));

            Pools pools = dao.retrievePools(cursor, pageSize, projectIdMask, nameMask, bottomMask,
                    topMask, isExplicit, isImplicit, office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Pools.class);

            String result = Formats.format(contentType, pools);

            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
            ctx.contentType(contentType.toString());

            byte[] bytes = result.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Pools", ex);
            logger.atSevere().withCause(ex).log("Failed to process request to retrieve Pools");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
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
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = Pool.class, type = Formats.JSONV2)
                            }
                    ),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the Location Category was not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested Pool", tags = {TAG})
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String poolId) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            PoolDao dao = new PoolDao(dsl);

            // These are required
            String office = requiredParam(ctx, OFFICE);
            String projectId = requiredParam(ctx, PROJECT_ID);

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
                logger.atInfo().log("%s%nfor request %s", re, ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeader(formatHeader, Pool.class);
                ctx.contentType(contentType.toString());

                String result = Formats.format(contentType, pool);

                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);

                byte[] bytes = result.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            }
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Pool", ex);
            logger.atSevere().withCause(ex).log("Failed to process request to retrieve Pool");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

    @OpenApi(
          requestBody = @OpenApiRequestBody(
                content = {
                      @OpenApiContent(from = Pool.class, type = Formats.JSONV2)
                },
                required = true),
          queryParams = {
                @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                      description = "Create will fail if provided ID already exists. Default: true"),
                @OpenApiParam(name = CREATE_POOL_NAME, type = Boolean.class,
                      description = "Create will create the pool name if it doesn't already exists. Default: true")
          },
          description = "Create CWMS Pool",
          method = HttpMethod.POST,
          tags = {TAG},
          responses = {
                @OpenApiResponse(status = STATUS_204, description = "Pool successfully stored to CWMS.")
          }
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class)
                  .getOrDefault(true);
            boolean createPoolName = ctx.queryParamAsClass(CREATE_POOL_NAME, Boolean.class)
                  .getOrDefault(true);
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Pool.class);
            Pool pool = Formats.parseContent(contentType, ctx.body(), Pool.class);
            PoolDao dao = new PoolDao(dsl);
            dao.createPool(pool, failIfExists, createPoolName);
            StatusResponse re = new StatusResponse(pool.getPoolName().getOfficeId(),
                  "Pool: " + pool.getPoolName().getPoolName() + " successfully created.",
                  pool.getPoolName().getPoolName());
            ctx.status(HttpServletResponse.SC_CREATED).json(re);
        }
    }

    @OpenApi(
          pathParams = {
                @OpenApiParam(name = NAME, description = "Specifies the name of "
                      + "the pool to be renamed."),
          },
          queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                      + "the pool to be renamed."),
                @OpenApiParam(name = NAME, required = true, description = "Specifies the new pool name. ")
          },
          description = "Rename CWMS Pool",
          method = HttpMethod.PATCH,
          tags = {TAG},
          responses = {
                @OpenApiResponse(status = STATUS_204, description = "Pool successfully renamed in CWMS.")
          }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String poolId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            String newName = requiredParam(ctx, NAME);
            DSLContext dsl = getDslContext(ctx);
            PoolDao dao = new PoolDao(dsl);
            dao.renamePool(office, poolId, newName);
            StatusResponse re = new StatusResponse(office, "Pool: " + poolId + " successfully renamed to: " + newName, newName);
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }

    @OpenApi(
          pathParams = {
                @OpenApiParam(name = NAME, description = "Specifies the name of "
                      + "the pool to be deleted."),
          },
          queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                      + "the pool to be deleted."),
                @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                      "Defaults to \"DELETE_KEY\"",
                      type = JooqDao.DeleteMethod.class)
          },
          description = "Delete CWMS Pool",
          method = HttpMethod.DELETE,
          tags = {TAG},
          responses = {
                @OpenApiResponse(status = STATUS_200, description = "Pool successfully deleted from CWMS."),
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                      + "inputs provided the pool was not found.")
          }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String poolId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            PoolDao dao = new PoolDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            String projectId = requiredParam(ctx, PROJECT_ID);
            dao.deletePool(office, projectId, poolId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
