package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListInput;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jooq.DSLContext;

public final class UpdateUserListController implements Handler {
    private final MetricRegistry metrics;

    public UpdateUserListController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        pathParams = @OpenApiParam(name = USER_LIST_ID, required = true,
            description = "The office-scoped list identifier."),
        queryParams = @OpenApiParam(name = OFFICE, required = true,
            description = "The office that owns the list."),
        requestBody = @OpenApiRequestBody(required = true,
            content = @OpenApiContent(from = UserListInput.class, type = Formats.JSON)),
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = @OpenApiContent(from = UserList.class, type = Formats.JSON)),
            @OpenApiResponse(status = "403", description = "Office administrator access required."),
            @OpenApiResponse(status = "404", description = "User list not found.")
        },
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "Update user-list metadata. Creator ownership is immutable.",
        method = HttpMethod.PATCH,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored =
                Controllers.markAndTime(metrics, getClass().getName(), UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            DSLContext dsl = UserListSupport.requireFeature(ctx, office);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            UserListInput input = ctx.bodyAsClass(UserListInput.class);
            ctx.json(dao.updateUserList(office,
                    UserListSupport.validateUserListId(ctx.pathParam(USER_LIST_ID)),
                    UserListSupport.validateDescription(input.getDescription())));
        }
    }
}
