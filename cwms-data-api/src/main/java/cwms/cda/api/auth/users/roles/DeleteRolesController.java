package cwms.cda.api.auth.users.roles;

import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.UserDao;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class DeleteRolesController implements Handler {
    private final MetricRegistry metrics;

    public DeleteRolesController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        
        pathParams = {
            @OpenApiParam(name = "office-id", required = true,
            description = "Office for these roles"),
            @OpenApiParam(name = "user-name", required = true,
                description = "Username of the user to alter")
        },
        responses = @OpenApiResponse(
                    status = STATUS_204
        ),
        requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = String[].class, type = Formats.JSON, isArray = true)
                    }
        ),
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "Remove roles from user",
        tags = {"User Management"}
    )
    @Override
    public void handle(Context ctx) throws Exception {
        final DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        final String user = ctx.pathParam("user-name");
        final String office = ctx.pathParam("office-id");
        final String[] roles = ctx.bodyAsClass(String[].class);
        UserDao dao = new UserDao(getDslContext(ctx));
        dao.deleteRoles(p, user, office, roles);
        ctx.status(HttpCode.NO_CONTENT);
    }
    
}
