package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.STATUS_201;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListInput;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jooq.DSLContext;

public final class CreateUserListController implements Handler {
    private final MetricRegistry metrics;

    public CreateUserListController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(required = true,
            content = @OpenApiContent(from = UserListInput.class, type = Formats.JSON)),
        responses = {
            @OpenApiResponse(status = STATUS_201,
                content = @OpenApiContent(from = UserList.class, type = Formats.JSON)),
            @OpenApiResponse(status = "403", description = "Office administrator access required."),
            @OpenApiResponse(status = "409", description = "The office already has this list ID.")
        },
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "Create an office-scoped user list owned by the authenticated user.",
        method = HttpMethod.POST,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored =
                Controllers.markAndTime(metrics, getClass().getName(), CREATE)) {
            UserListInput input = ctx.bodyAsClass(UserListInput.class);
            String office = UserListSupport.validateOffice(input.getOfficeId());
            String userListId = UserListSupport.validateUserListId(input.getUserListId());
            String description = UserListSupport.validateDescription(input.getDescription());
            DSLContext dsl = UserListSupport.requireFeature(ctx, office);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            UserList created = dao.createUserList(office, userListId, description,
                    UserListSupport.principal(ctx).getName());
            ctx.status(HttpCode.CREATED).json(created);
        }
    }
}
