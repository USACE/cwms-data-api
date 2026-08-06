package cwms.cda.api;

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
import cwms.cda.data.dao.AbstractForecastSpecDao;
import cwms.cda.data.dao.ForecastSpecDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.forecast.ForecastSpec;
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


public final class ForecastSpecController extends AbstractForecastSpecController<ForecastSpec> {

    public ForecastSpecController(MetricRegistry metrics) {
        super(metrics);
    }

    @Override
    protected AbstractForecastSpecDao<ForecastSpec> newDao(DSLContext dsl) {
        return new ForecastSpecDao(dsl);
    }

    @Override
    protected Class<ForecastSpec> getDtoClass() {
        return ForecastSpec.class;
    }

    @OpenApi(
            description = "Used to create and save forecast spec data",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)
                    },
                    required = true
            ),
            method = HttpMethod.POST,
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {
        super.create(ctx);
    }

    @OpenApi(
            description = "Used to delete forecast spec data based on unique fields",
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                        + "spec id of the forecast spec whose data is to be deleted."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the forecast spec whose data is to be deleted."),
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
        super.delete(ctx, name);
    }

    @OpenApi(
            description = "Used to query multiple forecast specs",
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the "
                        + "owning office of the forecast spec whose data is to be included in the "
                        + "response."),
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
                            @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)}),
                @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                        + "implemented")
            },
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        super.getAll(ctx);
    }

    @OpenApi(
            description = "Used to query a single forecast spec record",
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                        + "spec id of the forecast spec whose data is to be included in the response."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the forecast spec whose data is to be included in the "
                        + "response."),
                @OpenApiParam(name = DESIGNATOR, description = "Specifies the "
                        + "designator of the forecast spec whose data to be included in the response.")
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        description = "Returns the requested forecast spec",
                        content = {
                            @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)}),
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
        super.getOne(ctx, name);
    }

    @OpenApi(
            description = "Update a forecast spec with provided values",
            pathParams = {
                @OpenApiParam(name = NAME, description = "Forecast spec id to be updated")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)
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
        super.update(ctx, name);
    }
}
