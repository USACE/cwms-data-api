package cwms.cda.api.auth.users;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.UserDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.data.dto.auth.users.Users;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class UsersController implements CrudHandler {
    private final MetricRegistry metrics;
    private static final int DEFAULT_PAGE_SIZE = 100;
    public static final String TAG = "User Management";

    public UsersController(MetricRegistry metrics) {
        this.metrics = metrics;

    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String username) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }


    @OpenApi(
        queryParams = {
            @OpenApiParam(allowEmptyValue = true, name = OFFICE,
                    description = "Show only users with active privileges in a given office." 
                                + Controllers.OFFICE_DESCRIPTION ),
            @OpenApiParam(name = USERNAME_LIKE,
                    description = "Posix <a href=\"regexp.html\">regular expression</a> "
                            + " matching against the username"),
            @OpenApiParam(name = PAGE,
                    description = "This end point can return a lot of data, this "
                            + "identifies where in the request you are. This is an opaque"
                            + " value, and can be obtained from the 'next-page' value in "
                            + "the response."),
            @OpenApiParam(name = CURSOR, deprecated = true,
                    description = "This end point can return a lot of data, this "
                            + "identifies where in the request you are. This is an opaque"
                            + " value, and can be obtained from the 'next-page' value in "
                            + "the response. Deprecated, use " + PAGE + " instead."),
            @OpenApiParam(name = PAGE_SIZE,
                    type = Integer.class,
                    description = "How many entries per page returned. Default "
                            + DEFAULT_PAGE_SIZE + "."),
            @OpenApiParam(name = INCLUDE_ROLES,
                    type = Boolean.class,
                    allowEmptyValue = true,
                    description = "Include roles in the response. Default false.")
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = Users.class, type = Formats.JSON)
                    },
                    status = STATUS_200
        ),
        security = {
                @OpenApiSecurity(name = "gets overridden allows lock icon.")
            },
        description = "View all users",
        tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);
            String usernameRegex = ctx.queryParam(USERNAME_LIKE);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Users.class);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(UsersController.class.getName(), GET_ALL));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                        .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE}, Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(UsersController.class.getName(), GET_ALL));

            boolean includeRoles = queryParamAsClass(ctx, new String[]{INCLUDE_ROLES},
                    Boolean.class, false, metrics,
                    name(UsersController.class.getName(), GET_ALL));
            UserDao dao = new UserDao(dsl);
            Users users = dao.getAll(cursor, pageSize, office, includeRoles, usernameRegex);

            String result = Formats.format(contentType, users);

            ctx.result(result);
            ctx.contentType(contentType.toString());
        }
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = "user-name", required = true,
                description = "Specific user to retrieve")
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = User.class, type = Formats.JSON)
                    },
                    status = STATUS_200
        ),
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "View specific user",
        tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String userName) {
        DSLContext dsl = getDslContext(ctx);
        UserDao dao = new UserDao(dsl);
        User user = dao.getByUniqueName(userName, null).orElse(null);
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, User.class);
        String result = Formats.format(contentType, user);

        ctx.result(result);
        ctx.contentType(contentType.toString());
    }

    @OpenApi(
        ignore = true // users cannot be updated. Rolls are handled by a separate endpoint.
    )
    @Override
    public void update(Context ctx, String arg1) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }
}
