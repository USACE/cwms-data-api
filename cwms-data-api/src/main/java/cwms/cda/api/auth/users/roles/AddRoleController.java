package cwms.cda.api.auth.users.roles;

import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.UserDao;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;
import io.javalin.openapi.OpenApi;
import io.javalin.openapi.OpenApiContent;
import io.javalin.openapi.OpenApiParam;
import io.javalin.openapi.OpenApiRequestBody;
import io.javalin.openapi.OpenApiResponse;
import io.javalin.openapi.OpenApiSecurity;

public class AddRoleController implements Handler {
    private final MetricRegistry metrics;

    public AddRoleController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = "office-id", required = true,
            description = "Office for these roles." + Controllers.OFFICE_DESCRIPTION),
            @OpenApiParam(name = "user-name", required = true,
                description = "Name of the user to alter")
        },
        responses = {
            @OpenApiResponse(status = STATUS_204),
            @OpenApiResponse(status = STATUS_400,
                description = "One or more roles do not exist for the requested office.",
                content = @OpenApiContent(from = CdaError.class, type = Formats.JSON))
        },
        requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = String[].class, type = Formats.JSON)
                    }
        ),
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "Add roles to user",
        tags = {"User Management"},
        path = "/roles/add/{office-id}/{user-name}" // TODO: check
    )
    @Override
    public void handle(Context ctx) throws Exception {
        final DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        final String user = ctx.pathParam("user-name");
        final String office = ctx.pathParam("office-id");
        final String[] roles = ctx.bodyAsClass(String[].class);
        UserDao dao = new UserDao(getDslContext(ctx));
        dao.addRoles(p, user, office, roles);
        ctx.status(HttpStatus.NO_CONTENT);
    }
    
}
