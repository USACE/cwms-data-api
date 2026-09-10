package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jooq.DSLContext;

public final class DeleteUserListController implements Handler {
    private final MetricRegistry metrics;

    public DeleteUserListController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        pathParams = @OpenApiParam(name = USER_LIST_ID, required = true,
            description = "The office-scoped list identifier."),
        queryParams = @OpenApiParam(name = OFFICE, required = true,
            description = "The office that owns the list."),
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "User list deleted."),
            @OpenApiResponse(status = "403", description = "Office administrator access required."),
            @OpenApiResponse(status = "404", description = "User list not found.")
        },
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "Delete a user list and its membership rows.",
        method = HttpMethod.DELETE,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored =
                Controllers.markAndTime(metrics, getClass().getName(), DELETE)) {
            String office = requiredParam(ctx, OFFICE);
            DSLContext dsl = UserListSupport.requireFeature(ctx, office);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            dao.deleteUserList(office,
                    UserListSupport.validateUserListId(ctx.pathParam(USER_LIST_ID)));
            ctx.status(HttpCode.NO_CONTENT);
        }
    }
}
