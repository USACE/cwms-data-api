package cwms.cda.api.forecast;

import static cwms.cda.api.Controllers.DESIGNATOR;
import static cwms.cda.api.Controllers.DESIGNATOR_MASK;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.SOURCE_ENTITY;
import static cwms.cda.api.Controllers.SOURCE_ENTITY_LIKE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.forecast.ForecastSpecDao;
import cwms.cda.data.dao.forecast.ForecastSpecDaoV2;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.forecast.ForecastSpecV2;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class ForecastSpecControllerV2 extends ForecastSpecController<ForecastSpecV2> {

    public ForecastSpecControllerV2(MetricRegistry metrics) {
        super(metrics);
    }

    @Override
    protected ForecastSpecDao<ForecastSpecV2> newDao(DSLContext dsl) {
        return new ForecastSpecDaoV2(dsl);
    }

    @Override
    protected Class<ForecastSpecV2> getDtoClass() {
        return ForecastSpecV2.class;
    }

    @OpenApi(
            description = "Used to create and save forecast spec data",
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast spec to be created."),
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = ForecastSpecV2.class, type = Formats.JSON)
                    },
                    required = true
            ),
            method = HttpMethod.POST,
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {
        String officeFromPath = ctx.pathParam(OFFICE);
        validateOffice(ctx, officeFromPath);
        super.create(ctx);
    }

    @OpenApi(
            description = "Used to delete forecast spec data based on unique fields",
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast spec whose data is to be deleted."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "spec id of the forecast spec whose data is to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = DESIGNATOR, description = "Specifies the "
                            + "designator of the forecast spec whose data is to be deleted."),
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            responses = {
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a forecast spec."),
            },
            method = HttpMethod.DELETE,
            tags = TAG
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = ctx.pathParam(OFFICE);
        super.delete(ctx, name, office);
    }

    @OpenApi(
            description = "Used to query multiple forecast specs",
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast specs to be included in the "
                            + "response."),
            },
            queryParams = {
                    @OpenApiParam(name = ID_MASK, description = "Posix "
                            + "<a href=\"regexp.html\">regular expression</a>  that specifies "
                            + "the spec IDs to be included in the response."),
                    @OpenApiParam(name = DESIGNATOR_MASK, description = "Posix "
                            + "<a href=\"regexp.html\">regular expression</a>  that specifies the "
                            + "designator of the forecast spec whose data to be included in the response. "
                            + "Default behavior when this parameter is not provided is to search for forecast "
                            + "specifications with a null designator. "),
                    @OpenApiParam(name = SOURCE_ENTITY, description = "Specifies the source identity "
                            + "of the forecast spec whose data is to be included in the response. Interpreted as a regular expression."),
                    @OpenApiParam(name = SOURCE_ENTITY_LIKE, description = "Specifies the source entity using LIKE-style matching. If provided, this parameter is used instead of the regular expression parameter 'source-entity'.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            description = "A list of elements of the data set you've selected.",
                            content = {
                                    @OpenApiContent(from = ForecastSpecV2.class, type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String office = ctx.pathParam(OFFICE);
        super.getAll(ctx, office);
    }

    @OpenApi(
            description = "Used to query a single forecast spec record",
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast spec whose data is to be included in the "
                            + "response."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "spec id of the forecast spec whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = DESIGNATOR, description = "Specifies the "
                            + "designator of the forecast spec whose data to be included in the response.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            description = "Returns the requested forecast spec",
                            content = {
                                    @OpenApiContent(from = ForecastSpecV2.class, type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a forecast spec."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = ctx.pathParam(OFFICE);
        super.getOne(ctx, name, office);
    }

    @OpenApi(
            description = "Update a forecast spec with provided values",
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast spec to be updated."),
                    @OpenApiParam(name = NAME, description = "Forecast spec id to be updated")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = ForecastSpecV2.class, type = Formats.JSON)
                    },
                    required = true),
            responses = {
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the forecast spec was not found.")
            },
            method = HttpMethod.PATCH,
            tags = TAG
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        String officeFromPath = ctx.pathParam(OFFICE);
        validateOffice(ctx, officeFromPath);
        super.update(ctx, name);
    }

    private void validateOffice(@NotNull Context ctx, String officeFromPath) {
        if(officeFromPath == null) {
            throw new IllegalArgumentException("Office ID is required in the path parameter.");
        }
        ForecastSpecV2 body = deserializeForecastSpec(ctx);
        String officeFromBody = body.getSpecId().getOfficeId();
        if(!(officeFromPath.equalsIgnoreCase(officeFromBody))) {
            throw new IllegalArgumentException("Office ID in path parameter does not match office ID in request body.");
        }
    }
}
