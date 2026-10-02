package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserList;
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

public final class UserListController implements Handler {
    public static final String TAG = "User Management";
    private final MetricRegistry metrics;

    public UserListController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = USER_LIST_ID, required = true,
                description = "The identifier of the user list to retrieve.")
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true,
                description = "The office that owns the requested user list.")
        },
        responses = {
            @OpenApiResponse(
                status = STATUS_200,
                content = {
                    @OpenApiContent(from = UserList.class, type = Formats.JSON)
                }
            )
        },
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "Retrieve user list metadata.",
        methods = HttpMethod.GET,
        tags = {TAG},
        path = "/user-lists" // TODO: fix
    )
    @Override
    public void handle(Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String officeId = requiredParam(ctx, OFFICE);
            String userListId = UserListSupport.validateUserListId(ctx.pathParam(USER_LIST_ID));
            DSLContext dsl = getDslContext(ctx);
            if (!UserListFeature.requireSupported(ctx, dsl)) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserList userList = dao.getUserList(officeId, userListId)
                    .orElseThrow(() -> new NotFoundException("User list not found: "
                            + officeId + "/" + userListId));

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, UserList.class);
            String result = Formats.format(contentType, userList);

            ctx.result(result);
            ctx.contentType(contentType.toString());
        }
    }
}
