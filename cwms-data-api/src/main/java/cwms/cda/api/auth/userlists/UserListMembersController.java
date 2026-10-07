package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserListMembers;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.openapi.OpenApi;
import io.javalin.openapi.HttpMethod;
import io.javalin.openapi.OpenApiContent;
import io.javalin.openapi.OpenApiParam;
import io.javalin.openapi.OpenApiResponse;
import io.javalin.openapi.OpenApiSecurity;
import org.jooq.DSLContext;

public final class UserListMembersController implements Handler {
    public static final String TAG = "User Management";
    private final MetricRegistry metrics;

    public UserListMembersController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = USER_LIST_ID, required = true,
                description = "The identifier of the user list to retrieve members for.")
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true,
                description = "The office that owns the requested user list.")
        },
        responses = {
            @OpenApiResponse(
                status = STATUS_200,
                content = {
                    @OpenApiContent(from = UserListMembers.class, type = Formats.JSON)
                }
            ),
            @OpenApiResponse(status = STATUS_400, description = "A list parameter is missing or invalid."),
            @OpenApiResponse(status = STATUS_404, description = "User list not found."),
            @OpenApiResponse(status = STATUS_501, description = UserListFeature.UNSUPPORTED_MESSAGE)
        },
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "Retrieve the members of a user list.",
        methods = HttpMethod.GET,
        tags = {TAG},
        path = "/user/list/{" + USER_LIST_ID + "}/members"
    )
    @Override
    public void handle(Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String office = requiredParam(ctx, OFFICE);
            String userListId = UserListSupport.validateUserListId(ctx.pathParam(USER_LIST_ID));
            DSLContext dsl = getDslContext(ctx);
            if (!UserListFeature.requireSupported(ctx, dsl)) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListMembers members = dao.getMembers(office, userListId);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, UserListMembers.class);
            String result = Formats.format(contentType, members);

            ctx.result(result);
            ctx.contentType(contentType.toString());
        }
    }
}
