package cwms.cda.api.auth.users;

import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import java.security.Principal;
import java.util.Optional;

import org.jooq.DSLContext;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.UserDao;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class UserProfileController implements Handler {

    private final MetricRegistry metrics;

    public UserProfileController(MetricRegistry metrics) {
		this.metrics = metrics;
	}

	@OpenApi(
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = User.class, type = Formats.JSON)
                    },
                    status = STATUS_200
        ),
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "View users's own information",
        method = HttpMethod.GET,
        tags = {"User Management"}
    )
    @Override
    public void handle(Context ctx) throws Exception {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        DSLContext dsl = getDslContext(ctx);
        UserDao dao = new UserDao(dsl);
        String cac_user = p.getRoles()
                           .stream()
                           .filter(r -> r.equals(new Role(ApiServlet.CAC_USER)))
                           .map(r -> ApiServlet.CAC_USER)
                           .findFirst().orElse(null);
        User user = dao.getByUniqueName(p.getName(), cac_user).orElse(null);
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, User.class);
        String result = Formats.format(contentType, user);

        ctx.result(result);
        ctx.contentType(contentType.toString());
    }
    
}
