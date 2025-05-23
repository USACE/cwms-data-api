package cwms.cda.api.auth.users;

import static cwms.cda.api.Controllers.STATUS_200;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.data.dto.auth.users.User;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;

public class SelfUserController implements Handler {

    private final MetricRegistry metrics;

    public SelfUserController(MetricRegistry metrics) {
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handle'");
    }
    
}
