package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.ForecastSpecDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
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
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class ForecastSpecController implements CrudHandler {

    public static final String TAG = "Forecast";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ForecastSpecController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
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

            dao.create(forecastSpec);

            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to deserialize forecast spec from content body", ex);
        }
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
                @OpenApiParam(name = DESIGNATOR, required = true, description = "Specifies the "
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
        String designator = requiredParam(ctx, DESIGNATOR);

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
                        + "designator of the forecast spec whose data to be included in the response."),
                @OpenApiParam(name = SOURCE_ENTITY, description = "Specifies the source identity "
                        + "of the forecast spec whose data is to be included in the response.")
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
            String designator = ctx.queryParamAsClass(DESIGNATOR_MASK, String.class).getOrDefault("*");
            String sourceEntity = ctx.queryParamAsClass(SOURCE_ENTITY, String.class).getOrDefault("*");

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);

            List<ForecastSpec> specs = dao.getForecastSpecs(office, names, designator,
                    sourceEntity);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);
            String result = Formats.format(contentType, specs, ForecastSpec.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

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
                @OpenApiParam(name = DESIGNATOR, required = true, description = "Specifies the "
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
            String designator = requiredParam(ctx, DESIGNATOR);

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);

            ForecastSpec spec = dao.getForecastSpec(office, name, designator);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);
            String result = Formats.format(contentType, spec);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

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
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            ForecastSpec forecastSpec = deserializeForecastSpec(ctx);
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao dao = new ForecastSpecDao(dsl);
            dao.update(forecastSpec);
            ctx.status(HttpServletResponse.SC_OK);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to deserialize forecast spec from content body", ex);
        }
    }

    private ForecastSpec deserializeForecastSpec(Context ctx) throws IOException {
        return deserializeForecastSpec(ctx.body(), getUserDataContentType(ctx));
    }

    private ForecastSpec deserializeForecastSpec(String body, ContentType contentType)
            throws IOException {
        ForecastSpec retval;
        String type = contentType.toString();
        if ((Formats.JSONV2).equals(type)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, ForecastSpec.class);
        } else {
            throw new IOException("Unexpected format:" + type);
        }
        return retval;
    }

    private ContentType getUserDataContentType(@NotNull Context ctx) {
        String contentTypeHeader = ctx.req.getContentType();
        return Formats.parseHeader(contentTypeHeader);
    }

}
