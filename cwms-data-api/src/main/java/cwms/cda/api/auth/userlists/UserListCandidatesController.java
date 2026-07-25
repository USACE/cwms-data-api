package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.STATUS_200;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserListCandidates;
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

public final class UserListCandidatesController implements Handler {
    private static final String SEARCH = "search";
    private static final String PAGE_SIZE = "page-size";
    private final MetricRegistry metrics;

    public UserListCandidatesController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = SEARCH, required = true,
                description = "At least two characters from a user id, name, or email address."),
            @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                description = "Maximum candidates to return, from 1 through 50.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = @OpenApiContent(from = UserListCandidates.class, type = Formats.JSON))
        },
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "Search existing CWMS users for user-list membership.",
        method = HttpMethod.GET,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored =
                Controllers.markAndTime(metrics, getClass().getName(), GET_ALL)) {
            String search = UserListSupport.validateCandidateSearch(ctx.queryParam(SEARCH));
            int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(20);
            if (pageSize < 1 || pageSize > 50) {
                throw new IllegalArgumentException("page-size must be between 1 and 50");
            }
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            ctx.json(new UserListCandidates(
                    new UserListDao(dsl).searchCandidates(search, pageSize)));
        }
    }
}
