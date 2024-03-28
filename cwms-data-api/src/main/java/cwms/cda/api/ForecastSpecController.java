package cwms.cda.api;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesDeleteOptions;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.END_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.LOCATION;
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.api.Controllers.MAX_VERSION;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.SPEC_ID;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.SOURCE_ENTITY;
import static cwms.cda.api.Controllers.START_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.queryParamAsZdt;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.api.Controllers.requiredZdt;

public class ForecastSpecController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ForecastSpecController.class.getName());

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
            path = "/forecast-spec",
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            description = "Used to delete forecast spec data based on unique fields",
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the " +
                            "owning office of the forecast spec whose data is to be deleted."),
                    @OpenApiParam(name = SPEC_ID, required = true, description = "Specifies the " +
                            "spec if of the forecast spec whose data is to be deleted."),
                    @OpenApiParam(name = LOCATION, required = true, description = "Specifies the " +
                            "location of the forecast spec whose data is to be deleted."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a forecast spec."),
            },
            path = "/forecast-spec",
            method = HttpMethod.DELETE,
            tags = TAG
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String forecastSpecId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            description = "Used to query multiple forecast specs",
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the " +
                            "owning office of the forecast spec whose data is to be included in the " +
                            "response."),
                    @OpenApiParam(name = ID_MASK, required = true, description = "Posix "
                            + "<a href=\"regexp.html\">regular expression</a>  that specifies "
                            + "the spec IDs to be included in the response."),
                    @OpenApiParam(name = LOCATION_MASK, required = true, description = "Specifies the " +
                            "location of the forecast spec whose data to be included in the response."),
                    @OpenApiParam(name = SOURCE_ENTITY, description = "Specifies the source identity " +
                            "of the forecast spec whose data is to be included in the response.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            description = "A list of elements of the data set you've selected.",
                            content = {
                                    @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)}),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a forecast spec."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            path = "/forecast-spec/all",
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            description = "Used to query a single forecast spec record",
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the " +
                            "owning office of the forecast spec whose data is to be included in the " +
                            "response."),
                    @OpenApiParam(name = SPEC_ID, required = true, description = "Specifies the " +
                            "spec id of the forecast spec whose data is to be included in the response."),
                    @OpenApiParam(name = LOCATION, required = true, description = "Specifies the " +
                            "location of the forecast spec whose data to be included in the response."),
                    @OpenApiParam(name = SOURCE_ENTITY, description = "Specifies the source identity " +
                            "of the forecast spec whose data is to be included in the response.")
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
            path = "/forecast-spec",
            method = HttpMethod.GET,
            tags = TAG
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String id) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            description = "Update a forecast spec with provided values",
            pathParams = {
                    @OpenApiParam(name = SPEC_ID, description = "Forecast spec id to be updated")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = ForecastSpec.class, type = Formats.JSONV2)
                    },
                    required = true),
            responses = {
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the location was not found.")
            },
            method = HttpMethod.PATCH,
            path = "/forecast-spec/{spec-id}",
            tags = TAG
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

}
