package cwms.cda.api.auth.users.roles;

import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import java.util.List;

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
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class GetRolesController implements Handler {
    private final MetricRegistry metrics;

    public GetRolesController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = String[].class, type = Formats.JSON)
                    },
                    status = STATUS_200
        ),
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "View all roles",
        tags = {"User Management"}
    )
    @Override
    public void handle(Context ctx) throws Exception {
        final DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        UserDao dao = new UserDao(getDslContext(ctx));
        List<String> roles = dao.getRoles();
        ctx.json(roles).status(HttpCode.OK);
    }
    
}
