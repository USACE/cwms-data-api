package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserLists;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jooq.DSLContext;

public final class UserListsController implements Handler {
    private final MetricRegistry metrics;

    public UserListsController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        queryParams = @OpenApiParam(name = OFFICE, required = true,
            description = "The office whose user lists should be returned."),
        responses = @OpenApiResponse(status = STATUS_200,
            content = @OpenApiContent(from = UserLists.class, type = Formats.JSON)),
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "List office-scoped reusable user lists.",
        method = HttpMethod.GET,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored = Controllers.markAndTime(metrics, getClass().getName(), GET_ALL)) {
            String office = requiredParam(ctx, OFFICE);
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            ctx.json(new UserLists(new UserListDao(dsl).getUserLists(office)));
        }
    }
}
