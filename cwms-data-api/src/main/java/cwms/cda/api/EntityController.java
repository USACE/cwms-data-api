package cwms.cda.api;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.NotFoundException;
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
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class EntityController implements CrudHandler
{
    public static final Logger LOGGER = Logger.getLogger(EntityController.class.getName());
    public static final String TAG = "Entity";

    private final MetricRegistry metrics;


    public EntityController(MetricRegistry metrics)
    {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject)
    {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    // getAll() openApi setup
        // Parent_id, category_id and entity_name are optional parameters, not sure about how to use/handle matchNullParents
        // EntityDao, fromJooqEntityRecord() creates a CwmsId, which requires the office_id and name(entity_id)
    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = EntityDao.OFFICE_ID, description = " office id, ex: SPK"),
                    @OpenApiParam(name = EntityDao.ENTITY_ID, description = "ex: GOV or NWS"),
                    @OpenApiParam(name = EntityDao.PARENT_ENTITY_ID, description = "ex: NOAA"),
                    @OpenApiParam(name = CATEGORY_ID, description = "ex: GOV"),
                    @OpenApiParam(name = EntityDao.ENTITY_NAME, description = "ex: National Weather Service")

            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(isArray = true, from = Entity.class, type = Formats.JSON)}),
                    //TODO: if its ok to return an empty array then don't need 404
                    @OpenApiResponse(status = STATUS_404, description = "No matching entities found"),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid input or missing/invalid parameters"),
                    @OpenApiResponse(status = STATUS_501, description = "Request format is not supported")
            },
            description = "Returns all CWMS Entity Data filtered by optional masks.",
            tags = {TAG}
    )

    // getAll() should only have errors if required fields are missing, verify: handled with requiredParam, no need to catch
    @Override
    public void getAll(@NotNull Context ctx)
    {
        try (final Timer.Context ignored = markAndTime(GET_ALL))
        {
            DSLContext dsl = getDslContext(ctx);

            // Extract queryParams
            String officeId = ctx.queryParam(EntityDao.OFFICE_ID);
            String entityId = ctx.queryParam(EntityDao.ENTITY_ID);
            String parentId = ctx.queryParam(EntityDao.PARENT_ENTITY_ID);
            boolean matchNullParents = true; //TODO: how do I handle this variable?? tests show = true
            String categoryId = ctx.queryParam(CATEGORY_ID);
            String entityName = ctx.queryParam(EntityDao.ENTITY_NAME);

            // Instantiate DAO and call retrieveEntities
            EntityDao dao = new EntityDao(dsl);
            List<Entity> entities = dao.retrieveEntities(
                    officeId, entityId, parentId, matchNullParents, categoryId, entityName);

            // Format response
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            ctx.contentType(contentType.toString());
            String result = Formats.format(contentType, entities, Entity.class);

            // Return result
            if (entities.isEmpty())
            {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
            } else
            {
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            }
        } // no catch block //TODO: does it need to log error messages here?

    }

    // getOne() openApi setup
    // CwmsId is required, which needs the office_id and name, which here is entity_id
    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = EntityDao.ENTITY_ID, required = true, description = "Specifies the entity " +
                            "name as an id, example: NWS")
            },
            queryParams = {
                    @OpenApiParam(name = EntityDao.OFFICE_ID, required = true, description = "Office id, ex: SPK")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = Entity.class, type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_404, description = "Entity not found"),
                    @OpenApiResponse(status = STATUS_400, description = "Missing Required parameter: office_id")
            },
            description = "Returns a single CWMS Entity by entity id and office id.",
            tags = {TAG}
    )

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String entityId)
    {
        try (final Timer.Context ignored = markAndTime(GET_ONE))
        {
            DSLContext dsl = getDslContext(ctx);
            // Extract required queryParams
            String officeId = requiredParam(ctx, EntityDao.OFFICE_ID);

            // build cwmsId and Instantiate DAO
            CwmsId cwmsId = new CwmsId.Builder()
                    .withOfficeId(officeId)
                    .withName(entityId)
                    .build();

            EntityDao dao = new EntityDao(dsl);
            // throws a NotFoundException
            try
            {
                Entity foundEntity = dao.retrieveEntity(cwmsId);
                // format response
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
                ctx.contentType(contentType.toString());
                String result = Formats.format(contentType, foundEntity);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            } catch (NotFoundException e)
            {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
            }

        }
    }

    // create() openApi setup
    @OpenApi(
            description = "Create new Entity",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = Entity.class, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided entity ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            path = "/entities",
            tags = {TAG}
    )

    @Override
    public void create(@NotNull Context ctx)
    {
        try (final Timer.Context ignored = markAndTime(CREATE))
        {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
            Entity entity = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Entity.class);
            EntityDao dao = new EntityDao(dsl);
            dao.createEntity(entity);
            ctx.status(HttpServletResponse.SC_CREATED);


        }
    }

    // update() openApi setup
    @OpenApi(
            description = "Update an existing Entity",
            requestBody = @OpenApiRequestBody(
                    content = {@OpenApiContent(from = Entity.class, type = Formats.JSON)},
                    required = true),
            pathParams = {
                    @OpenApiParam(name = EntityDao.ENTITY_ID, required = true, description = "Specifies the entity " +
                            "name as an id, example: NWS")
            },
            method = HttpMethod.PATCH,
            path = "/entity",
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_200, description = "Entity updated successfully"),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of inputs provided"
                    + "the entity was not found.")
            }
    )

    // throws exception, non specific
    @Override
    public void update(@NotNull Context ctx, @NotNull String entityId)
    {
        try (final Timer.Context ignored = markAndTime(UPDATE))
        {
            DSLContext dsl = getDslContext(ctx);
            String originalEntityId = ctx.queryParam(EntityDao.ENTITY_ID);

            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Entity.class);
//            Entity entity = Formats.parseContent(contentType, ctx.body(), Entity.class); //TODO: some classes use this??
            Entity entity = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Entity.class);

            CwmsId entityCwmsId = entity.getId();
            String officeId = ctx.queryParam(EntityDao.OFFICE_ID);

            if (originalEntityId != null && originalEntityId.equals(entityId)) {
                ctx.status(HttpServletResponse.SC_EXPECTATION_FAILED);
            }

            EntityDao dao = new EntityDao(dsl);
            dao.updateEntity(entity);
            ctx.status(HttpServletResponse.SC_OK);

            //TODO: need to verify what exception would be thrown here
            //TODO: need to see how you verify that the update was successful

        }
    }

    // delete() openApi setup
    @OpenApi(
            description = "Deletes specified entity",
            pathParams = {
                    @OpenApiParam(name = EntityDao.ENTITY_ID, required = true, description = "Specifies the entity " +
                            "name as an id, example: NWS")
            },
            queryParams = {
                    @OpenApiParam(name = EntityDao.OFFICE_ID, description = "Office id, ex: SPK"),
                    @OpenApiParam(name = "deleteChildren", type = Boolean.class, description = "Delete all children"
                            + " of the entity")

            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )

    // throws a notfoundexception if no entity id
    @Override
    public void delete(@NotNull Context ctx, @NotNull String entityId)
    {
        try (final Timer.Context ignored = markAndTime(DELETE))
        {
            DSLContext dsl = getDslContext(ctx);
            String officeId = ctx.queryParam(EntityDao.OFFICE_ID);
            boolean deleteAll = true; //TODO: need to figure out how this happens- when/how is the user asked to do this
            // TODO: not sure you can delete just a child entity..
            // build cwmsId and Instantiate DAO
            CwmsId cwmsId = new CwmsId.Builder()
                    .withOfficeId(officeId)
                    .withName(entityId)
                    .build();

            EntityDao dao = new EntityDao(dsl);
            dao.deleteEntity(cwmsId, deleteAll);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }




}
