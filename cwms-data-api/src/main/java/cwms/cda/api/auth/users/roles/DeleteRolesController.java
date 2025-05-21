package cwms.cda.api.auth.users.roles;

import static cwms.cda.api.Controllers.STATUS_204;

import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class DeleteRolesController implements Handler {

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = "user-name", required = true,
                description = "Username of the user to alter")
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = Void.class, type = "")
                    },
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handle'");
    }
    
}
