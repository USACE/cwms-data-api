package cwms.cda.api.auth.users;

import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.CwmsDataApi;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.UserDao;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import io.javalin.http.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.openapi.OpenApi;
import io.javalin.openapi.HttpMethod;
import io.javalin.openapi.OpenApiContent;
import io.javalin.openapi.OpenApiResponse;
import io.javalin.openapi.OpenApiSecurity;
import jakarta.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

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
        description = "View users' own information",
        methods = HttpMethod.GET,
        tags = {"User Management"},
        path = "/user/profile"
    )
    @Override
    public void handle(Context ctx) throws Exception {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        DSLContext dsl = getDslContext(ctx);
        UserDao dao = new UserDao(dsl);
        String cac_user = p.getRoles()
                           .stream()
                           .filter(r -> r.equals(new Role(CwmsDataApi.CAC_USER)))
                           .map(r -> CwmsDataApi.CAC_USER)
                           .findFirst().orElse(null);
        User user = dao.getByUniqueName(p.getName(), cac_user).orElse(null);
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, User.class);
        String result = Formats.format(contentType, user);

        ctx.contentType(contentType.toString());
        ctx.status(HttpServletResponse.SC_OK);

        byte[] bytes = result.getBytes();
        ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
        ctx.outputStream().write(bytes);
    }
    
}
