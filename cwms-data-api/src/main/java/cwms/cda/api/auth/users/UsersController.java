package cwms.cda.api.auth.users;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_201;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import org.jooq.DSLContext;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.UserDao;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.security.Role;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.security.RouteRole;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class UsersController implements CrudHandler {
    private final MetricRegistry metrics;


    public UsersController(MetricRegistry metrics) {
        this.metrics = metrics;

    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = User.class, type = Formats.JSON)
                    }
        ),
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = User.class, type = Formats.JSON)
                    },
                    status = STATUS_201
        ),
        description = "Create a new User",
        tags = {"User Management"}
    )
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Unimplemented method 'create'");
    }

    @OpenApi(
        responses = @OpenApiResponse(
                    status = STATUS_204
        ),
        description = "Delete API key for a user",
        tags = {"User Management"}
    )
    @Override
    public void delete(Context ctx, String username) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }


    @OpenApi(
        queryParams = {
            @OpenApiParam(allowEmptyValue = true, name = "office-id", type = String.class,
                          description = "Show only users with active privileges in a given office" )
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = User[].class, type = Formats.JSON)
                    },
                    status = STATUS_200
        ),
        security = {
                @OpenApiSecurity(name = "gets overridden allows lock icon.")
            },
        description = "View all users",
        tags = {"User Management"}
    )
    @Override
    public void getAll(Context ctx) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAll'");
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
        tags = {"User Management"}
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
        throw new UnsupportedOperationException("Unimplemented method 'update'");
    }

    
    
}
