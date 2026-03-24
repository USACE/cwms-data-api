package cwms.cda.api;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.ForecastSpecDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static cwms.cda.api.Controllers.*;

public final class ForecastSpecController extends BaseCrudHandler {

    public static final String TAG = "Forecast";

    public ForecastSpecController(MetricRegistry metrics) {
        super(metrics);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
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
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);
            ForecastSpec forecastSpec = deserializeForecastSpec(ctx);

            try {
                ForecastSpec existing = dao.getForecastSpec(forecastSpec.getOfficeId(), forecastSpec.getSpecId(), forecastSpec.getDesignator());
                if(locationsAreDifferent(forecastSpec, existing)) {
                    dao.updateSpecWithLocationIdChange(forecastSpec);
                } else {
                    dao.create(forecastSpec);
                }
            } catch (NotFoundException e) {
                dao.create(forecastSpec);
            }

            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    private static boolean locationsAreDifferent(ForecastSpec forecastSpec, ForecastSpec existing) {
        String newLocationId = forecastSpec.getLocationId();
        String existingLocationId = existing.getLocationId();
        return (newLocationId == null && existingLocationId != null)
                || (newLocationId != null && !newLocationId.equalsIgnoreCase(existingLocationId));
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
        String office = requiredParam(ctx, OFFICE);
        String designator = ctx.queryParamAsClass(DESIGNATOR, String.class).allowNullable().get();

        JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
        DeleteRule deleteRule;
        switch (deleteMethod) {
            case DELETE_ALL:
                deleteRule = DeleteRule.DELETE_ALL;
                break;
            case DELETE_DATA:
                deleteRule = DeleteRule.DELETE_DATA;
                break;
            case DELETE_KEY:
                deleteRule = DeleteRule.DELETE_KEY;
                break;
            default:
                throw new IllegalArgumentException("Delete Method provided does not match accepted rule constants: "
                        + deleteMethod);
        }
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);

            dao.delete(office, name, designator, deleteRule);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
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
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            String office = ctx.queryParam(OFFICE);
            String names = ctx.queryParamAsClass(ID_MASK, String.class).getOrDefault("*");
            String designator = ctx.queryParamAsClass(DESIGNATOR_MASK, String.class).allowNullable().get();
            String sourceEntity = ctx.queryParamAsClass(SOURCE_ENTITY, String.class).getOrDefault("*");
            String entityLike = ctx.queryParamAsClass(SOURCE_ENTITY_LIKE, String.class).allowNullable().get();

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);

            List<ForecastSpec> specs = dao.getForecastSpecs(office, names, designator,
                    sourceEntity, entityLike);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, ForecastSpec.class);
            String result = Formats.format(contentType, specs, ForecastSpec.class);

            ctx.result(result).contentType(contentType.toString());
            updateResultSize(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
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
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String office = requiredParam(ctx, OFFICE);
            String designator = ctx.queryParamAsClass(DESIGNATOR, String.class).allowNullable().get();

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);

            ForecastSpec spec = dao.getForecastSpec(office, name, designator);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, ForecastSpec.class);
            String result = Formats.format(contentType, spec);

            ctx.result(result).contentType(contentType.toString());
            updateResultSize(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
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
        logUnusedPathParameter(ctx, NAME, "Body contains information");
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            ForecastSpec forecastSpec = deserializeForecastSpec(ctx);
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);
            ForecastSpec existing = dao.getForecastSpec(forecastSpec.getOfficeId(), forecastSpec.getSpecId(), forecastSpec.getDesignator());
            if(locationsAreDifferent(forecastSpec, existing)) {
                dao.updateSpecWithLocationIdChange(forecastSpec);
            } else {
                dao.create(forecastSpec);
            }
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    private ForecastSpec deserializeForecastSpec(Context ctx) {
        String formatHeader = ctx.req.getContentType();
        ContentType contentType = Formats.parseHeader(formatHeader, ForecastSpec.class);
        return Formats.parseContent(contentType, ctx.body(), ForecastSpec.class);
    }

}
