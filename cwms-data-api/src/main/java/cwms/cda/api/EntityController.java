package cwms.cda.api;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.EntityDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Entity;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.*;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class EntityController implements CrudHandler {
    private static final String TAG = "Entity";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;


    public EntityController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram(name(className, "results", "size"));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            description = "Returns all CWMS Entity Data filtered by optional masks.",
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Office ID to filter entities (e.g., SPK). If omitted, " +
                            "returns entities for all offices."),
                    @OpenApiParam(name = ENTITY_ID, description = "Entity ID to filter by specific entity. If omitted, " +
                            "returns all entities. (e.g., GOV or NWS)."),
                    @OpenApiParam(name = PARENT_ENTITY_ID, description = "Parent Entity ID to filter entities " +
                            "by parent (e.g., NOAA)."),
                    @OpenApiParam(name = CATEGORY_ID, description = "Category ID to filter entities by category (e.g., GOV)."),
                    @OpenApiParam(name = LONG_NAME, description = "Entity long name to filter entities " +
                            "(e.g., National Weather Service)."),
                    @OpenApiParam(name = MATCH_NULL_PARENTS, type = Boolean.class, description = "If true, include " +
                            "entities with null parent IDs. Default is true.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, from = Entity.class, type = Formats.JSONV2)
                    })
            },
            tags = {TAG}
    )

    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            String officeId = ctx.queryParam(OFFICE);
            String entityId = ctx.queryParam(ENTITY_ID);
            String parentId = ctx.queryParam(PARENT_ENTITY_ID);
            Boolean matchNullParents = ctx.queryParamAsClass(MATCH_NULL_PARENTS, Boolean.class)
                    .getOrDefault(true);
            String categoryId = ctx.queryParam(CATEGORY_ID);
            String entityName = ctx.queryParam(LONG_NAME);

            EntityDao dao = new EntityDao(dsl);
            List<Entity> entities = dao.retrieveEntities(
                    officeId, entityId, parentId, matchNullParents, categoryId, entityName);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            ctx.contentType(contentType.toString());
            String result = Formats.format(contentType, entities, Entity.class);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(result.length());
        }
    }

    @OpenApi(
            description = "Returns CWMS Entity data by entity id and office id.",
            pathParams = {
                    @OpenApiParam(name = ENTITY_ID, required = true, description = "Specifies the Entity ID of the entity to be " +
                            " retrieved. (e.g., NWS)."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the entity to be retrieved. e.g., SPK)")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                                    @OpenApiContent(from = Entity.class, type = Formats.JSONV2)
                    })
            },
            tags = {TAG}
    )

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String entityId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            String officeId = requiredParam(ctx, OFFICE);

            CwmsId cwmsId = new CwmsId.Builder()
                    .withOfficeId(officeId)
                    .withName(entityId)
                    .build();

            EntityDao dao = new EntityDao(dsl);
            Entity foundEntity = dao.retrieveEntity(cwmsId);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            ctx.contentType(contentType.toString());
            String result = Formats.format(contentType, foundEntity);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(result.length());
        }
    }

    @OpenApi(
            description = "Create CWMS Entity",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = Entity.class, type = Formats.JSONV2)
                    },
                    required = true),
            responses = {
                    @OpenApiResponse(status = STATUS_201, description = "Entity successfully stored to CWMS")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )

    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            Entity entity = Formats.parseContent(contentType, ctx.body(), Entity.class);
            EntityDao dao = new EntityDao(dsl);
            dao.createEntity(entity);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    // update() openApi setup
    @OpenApi(
            description = "Update an existing Entity.",
            requestBody = @OpenApiRequestBody(
                    content = {@OpenApiContent(from = Entity.class, type = Formats.JSONV2)},
                    required = true),
            pathParams = {
                    @OpenApiParam(name = ENTITY_ID, required = true, description = "Specifies the entity ID of the " +
                            " Entity to be updated. (e.g., NWS)")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office "+
                            " of the entity to be updated. (e.g., SPK)")
            },
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_200, description = "Entity updated successfully in CWMS"),
            }
    )

    @Override
    public void update(@NotNull Context ctx, @NotNull String entityId) {
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);
            String officeId = requiredParam(ctx, OFFICE);
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            Entity entity = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Entity.class);
            if (entity.getId() == null || entity.getId().getOfficeId() == null || entity.getId().getName() == null) {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Entity ID and Office ID must be provided in the request body.");
                return;
            }

            if (!entityId.equalsIgnoreCase(entity.getId().getName())) {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
                ctx.result("Entity not found for the given entity-id.");
                return;
            }

            if (!officeId.equalsIgnoreCase(entity.getId().getOfficeId())) {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Office ID in query parameter must match the Office ID in the request body.");
                return;
            }

            EntityDao dao = new EntityDao(dsl);
            dao.updateEntity(entity);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }


        @OpenApi(
            description = "Delete CWMS Entity.",
            pathParams = {
                    @OpenApiParam(name = ENTITY_ID, required = true, description = "Specifies the entity ID " +
                            "of the Entity to be deleted (e.g., NWS).")

            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office "+
                            " of the entity to be updated. (e.g., SPK)"),
                    @OpenApiParam(name = CASCADE_DELETE, required = true, type = Boolean.class,
                            description = "If true, also delete all descendant child entities.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Entity deleted successfully"),
                    @OpenApiResponse(status = STATUS_404, description = "Entity not found for the given parameters."),
            },
            method = HttpMethod.DELETE,
            tags = {TAG}

    )

    @Override
    public void delete(@NotNull Context ctx, @NotNull String entityId) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String officeId = requiredParam(ctx, OFFICE);
            Boolean deleteEntityAndChildren = requiredParamAs(ctx, CASCADE_DELETE, Boolean.class);

            CwmsId cwmsId = new CwmsId.Builder()
                    .withOfficeId(officeId)
                    .withName(entityId)
                    .build();

            EntityDao dao = new EntityDao(dsl);
            dao.deleteEntity(cwmsId, deleteEntityAndChildren);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
