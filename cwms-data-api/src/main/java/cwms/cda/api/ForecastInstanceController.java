package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.DESIGNATOR;
import static cwms.cda.api.Controllers.DESIGNATOR_MASK;
import static cwms.cda.api.Controllers.FORECAST_DATE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ISSUE_DATE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.ForecastInstanceDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.forecast.ForecastInstance;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

public class ForecastInstanceController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ForecastInstanceController.class.getName());

    public static final String TAG = "Forecast";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ForecastInstanceController(MetricRegistry metrics) {
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
            description = "Used to create and save a forecast instance",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = ForecastInstance.class, type = Formats.JSONV2)
                    },
                    required = true
            ),
            method = HttpMethod.POST,
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));

            ForecastInstance forecastInstance = deserializeForecastInstance(ctx);
            dao.create(forecastInstance);
            ctx.status(HttpServletResponse.SC_OK);
        } catch (IOException | DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
            description = "Used to delete forecast instance data based on unique fields",
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "spec id of the forecast spec associated with the forecast instance"
                            + "to be deleted."),
            },
            queryParams = {
                @OpenApiParam(name = FORECAST_DATE, required = true, description = "Specifies the "
                        + "forecast date time of the forecast instance to be deleted."),
                @OpenApiParam(name = ISSUE_DATE, required = true, description = "Specifies the "
                        + "issue date time of the forecast instance to be deleted."),
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the forecast spec associated with the forecast instance "
                        + "to be deleted."),
                @OpenApiParam(name = DESIGNATOR, required = true, description = "Specifies the "
                        + "designator of the forecast spec associated with the forecast instance "
                        + "to be deleted."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                        + "parameters did not find a forecast instance."),
            },
            method = HttpMethod.DELETE,
            tags = TAG
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);

        String designator = requiredParam(ctx, DESIGNATOR);
        String forecastDate =  requiredParam(ctx, FORECAST_DATE);
        String issueDate = requiredParam(ctx, ISSUE_DATE);
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));
            dao.delete(office, name, designator, forecastDate, issueDate);
        }
    }

    @OpenApi(
            description = "Used to get all forecast instances for a given forecast spec",
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the "
                        + "owning office of the forecast spec whose forecast instance is to be "
                        + "included in the response. Default will be all offices."),
                @OpenApiParam(name = NAME, description = "Specifies the "
                        + "spec id of the forecast spec whose forecast instance data is to be "
                        + "included in the response. Default will be all names."),
                @OpenApiParam(name = DESIGNATOR, description = "Specifies the "
                        + "designator of the forecast spec whose forecast instance data to be included "
                        + "in the response. "),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        description = "A list of elements of the data set you've selected.",
                        content = {
                            @OpenApiContent(from = ForecastInstance.class, type = Formats.JSONV2)}),
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
            String desionatorMask = ctx.queryParam(DESIGNATOR_MASK);
            String name = ctx.queryParam(NAME);

            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));
            List<ForecastInstance> instances = dao.getForecastInstances(office, name, desionatorMask);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);
            String result = Formats.format(contentType, instances, ForecastInstance.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
            description = "Used to get all forecast instances for a given forecast spec",
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                        + "spec id of the forecast spec whose forecast instance data is to be "
                        + "included in the response."),
            },
            queryParams = {
                @OpenApiParam(name = FORECAST_DATE, required = true, description = "Specifies the "
                        + "forecast date time of the forecast instance to be retrieved."),
                @OpenApiParam(name = ISSUE_DATE, required = true, description = "Specifies the "
                        + "issue date time of the forecast instance to be retrieved."),
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the forecast spec whose forecast instance is to be "
                        + "included in the response."),
                @OpenApiParam(name = DESIGNATOR, required = true, description = "Specifies the "
                        + "designator of the forecast spec whose forecast instance data to be included "
                        + "in the response."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        description = "A list of elements of the data set you've selected.",
                        content = {
                            @OpenApiContent(from = ForecastInstance.class, type = Formats.JSONV2)}),
                @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                        + "parameters did not find a forecast instance."),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                        + "implemented")
            },
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        String designator = requiredParam(ctx, DESIGNATOR);
        String forecastDate =  requiredParam(ctx, FORECAST_DATE);
        String issueDate = requiredParam(ctx, ISSUE_DATE);
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));
            ForecastInstance instance = dao.getForecastInstance(office, name, designator, forecastDate, issueDate);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);
            String result = Formats.format(contentType, instance);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
            description = "Update a forecast instance with new max age, notes, forecast file"
                    + "and forecast info key/value pairs.",
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                        + "spec id of the forecast spec to be updated."),
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = ForecastInstance.class, type = Formats.JSONV2)
                    },
                    required = true),
            responses = {
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                        + "inputs provided the ForecastInstance was not found.")
            },
            method = HttpMethod.PATCH,
            tags = TAG
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            ForecastInstance forecastInstance = deserializeForecastInstance(ctx);
            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));
            dao.update(forecastInstance);
        } catch (IOException | DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private ForecastInstance deserializeForecastInstance(Context ctx) throws IOException {
        return deserializeForecastInstance(ctx.body(), getUserDataContentType(ctx));
    }

    private ForecastInstance deserializeForecastInstance(String body, ContentType contentType)
            throws IOException {
        return deserializeForecastInstance(body, contentType.toString());
    }

    public static ForecastInstance deserializeForecastInstance(String body, String contentType)
            throws IOException {
        ForecastInstance retval;

        if ((Formats.JSONV2).equals(contentType)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, ForecastInstance.class);
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    private ContentType getUserDataContentType(@NotNull Context ctx) {
        String contentTypeHeader = ctx.req.getContentType();
        return Formats.parseHeader(contentTypeHeader);
    }

}
