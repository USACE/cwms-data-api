package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_201;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserListMember;
import cwms.cda.data.dto.auth.userlists.UserListMemberInput;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jooq.DSLContext;

public final class AddUserListMemberController implements Handler {
    private final MetricRegistry metrics;

    public AddUserListMemberController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        pathParams = @OpenApiParam(name = USER_LIST_ID, required = true,
            description = "The office-scoped list identifier."),
        queryParams = @OpenApiParam(name = OFFICE, required = true,
            description = "The office that owns the list."),
        requestBody = @OpenApiRequestBody(required = true,
            content = @OpenApiContent(from = UserListMemberInput.class, type = Formats.JSON)),
        responses = {
            @OpenApiResponse(status = STATUS_201,
                content = @OpenApiContent(from = UserListMember.class, type = Formats.JSON)),
            @OpenApiResponse(status = "403", description = "Office administrator access required."),
            @OpenApiResponse(status = "404", description = "List or CWMS user not found."),
            @OpenApiResponse(status = "409", description = "User is already a member.")
        },
        security = @OpenApiSecurity(name = "gets overridden allows lock icon."),
        description = "Add an existing CWMS user to an office-scoped user list.",
        method = HttpMethod.POST,
        tags = UserListController.TAG
    )
    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored =
                Controllers.markAndTime(metrics, getClass().getName(), CREATE)) {
            String office = requiredParam(ctx, OFFICE);
            DSLContext dsl = UserListSupport.requireFeature(ctx, office);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            UserListMemberInput input = ctx.bodyAsClass(UserListMemberInput.class);
            String userId = UserListSupport.validateUserId(input.getUserId());
            ctx.status(HttpCode.CREATED).json(dao.addMember(
                    office, UserListSupport.validateUserListId(ctx.pathParam(USER_LIST_ID)), userId,
                    UserListSupport.principal(ctx).getName()));
        }
    }
}
