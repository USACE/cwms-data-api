package cwms.cda.api.rating;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.SPEC_ID_MASK;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import cwms.cda.data.dao.RatingSpecDao;
import cwms.cda.data.dto.rating.RatingEffectiveDatesMap;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class RatingEffectiveDatesController implements Handler {

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;
    public RatingEffectiveDatesController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime() {
        return Controllers.markAndTime(metrics, getClass().getName(), Controllers.GET_ALL);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description =
                            "Office Id used to filter the results."),
                    @OpenApiParam(name = SPEC_ID_MASK, description =
                            "Spec Id used to filter the results. " +
                            "Defaults to '*'")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(type = Formats.JSONV1, from = RatingEffectiveDatesMap.class)
                    })
            },
            description = "Returns mapping of office -> spec id -> effective date-times for all matching offices and spec ids.",
            tags = {RatingController.TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String officeIdMask = ctx.queryParam(OFFICE_MASK);
        String specIdMask = ctx.queryParamAsClass(SPEC_ID_MASK, String.class).getOrDefault("*");
        try (Timer.Context ignored = markAndTime()) {
            DSLContext dsl = getDslContext(ctx);
            RatingSpecDao dao = new RatingSpecDao(dsl);
            RatingEffectiveDatesMap effectiveDatesForSpecs = dao.retrieveSpecEffectiveDates(officeIdMask, specIdMask);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, RatingEffectiveDatesMap.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, effectiveDatesForSpecs);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }
}
