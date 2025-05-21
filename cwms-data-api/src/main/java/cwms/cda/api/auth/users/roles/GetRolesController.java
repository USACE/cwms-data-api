package cwms.cda.api.auth.users.roles;

import static cwms.cda.api.Controllers.STATUS_200;

import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class GetRolesController implements Handler {

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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handle'");
    }
    
}
