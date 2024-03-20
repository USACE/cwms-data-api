package cwms.cda.api;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.JooqDao;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import java.util.logging.Logger;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;

public class ForecastSpecController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ForecastSpecController.class.getName());

    public static final String TAG = "ForecastSpec";
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

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {

    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String forecastSpecId) {

    }

    @OpenApi(ignore = true)
    @Override
    public void getAll(@NotNull Context ctx) {

    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String id) {

    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {


    }

}
