package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.USER_LIST_ID;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.api.errors.RequiredQueryParameterException;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListInput;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
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
        method = HttpMethod.GET,
        tags = {TAG}
    )
    @Override
    public void handle(Context ctx) {
        if ("PATCH".equals(ctx.method())) {
            update(ctx);
        } else if ("DELETE".equals(ctx.method())) {
            delete(ctx);
        } else {
            get(ctx);
        }
    }

    private void get(Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String office = ctx.queryParam(OFFICE);
            if (office == null || office.isBlank()) {
                throw new RequiredQueryParameterException(OFFICE);
            }

            final String officeId = ctx.queryParamAsClass(OFFICE, String.class)
                    .check(Office::validOfficeNotNull, "Invalid office provided")
                    .get();

            String userListId = ctx.pathParam(USER_LIST_ID);
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

    private void update(Context ctx) {
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            String office = UserListSupport.requiredOffice(ctx);
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            UserListInput input = ctx.bodyAsClass(UserListInput.class);
            ctx.json(dao.updateUserList(office, ctx.pathParam(USER_LIST_ID),
                    input.getDescription()));
        }
    }

    private void delete(Context ctx) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            String office = UserListSupport.requiredOffice(ctx);
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            dao.deleteUserList(office, ctx.pathParam(USER_LIST_ID));
            ctx.status(HttpCode.NO_CONTENT);
        }
    }
}
