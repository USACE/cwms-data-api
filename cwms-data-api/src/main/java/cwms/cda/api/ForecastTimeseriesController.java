package cwms.cda.api;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.forecast.ForecastInstance;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import java.util.logging.Logger;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CREATE_AS_LRTS;
import static cwms.cda.api.Controllers.FORECAST_DATE_TIME;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ISSUE_DATE_TIME;
import static cwms.cda.api.Controllers.LOCATION;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.SPEC_ID;
import static cwms.cda.api.Controllers.STORE_RULE;
import static cwms.cda.api.Controllers.TIMESERIES_ID;
import static cwms.cda.api.Controllers.TIMEZONE;

public class ForecastTimeseriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ForecastSpecController.class.getName());

    public static final String TAG = "Forecast";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ForecastTimeseriesController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            description = "Used to create and save a forecast timeseries",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2)
                    },
                    required = true
            ),
            queryParams = {
                    @OpenApiParam(name = FORECAST_DATE_TIME, required = true, description = "Specifies the " +
                            "forecast date time of the forecast instance to be associated with the created" +
                            "forecast timeseries."),
                    @OpenApiParam(name = ISSUE_DATE_TIME, required = true, description = "Specifies the " +
                            "issue date time of the forecast instance to be associated with the created " +
                            "forecast timeseries."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the " +
                            "owning office of the forecast spec whose forecast instance will be " +
                            "associated with the created forecast timeseries."),
                    @OpenApiParam(name = SPEC_ID, required = true, description = "Specifies the " +
                            "spec id of the forecast spec whose forecast instance will be " +
                            "associated with the created forecast timeseries."),
                    @OpenApiParam(name = LOCATION, required = true, description = "Specifies the " +
                            "location of the forecast spec whose forecast instance will be" +
                            "associated with the created forecast timeseries."),
                    @OpenApiParam(name = TIMESERIES_ID, required = true, description = "Id of timeseries " +
                            "that will be created.")
            },
            method = HttpMethod.POST,
            path = "/forecast-timeseries",
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {

    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String forecastSpecId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String id) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }

    }

}

