package cwms.cda.api.auth;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import java.util.List;

import org.jooq.DSLContext;

public class ApiKeyController implements CrudHandler {
    public final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ApiKeyController(MetricRegistry metrics) {
        this.metrics = metrics;

        requestResultSize = this.metrics.histogram((name(this.getClass().getName(), RESULTS, SIZE)));
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = ApiKey.class, type = Formats.JSON)
                    }
        ),
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey.class, type = Formats.JSON)
                    },
                    status = "201"
        ),
        description = "Create, View, or Delete API keys for a user",
        tags = {"Authorization"}
    )
    @Override
    public void create(Context ctx) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        try(DSLContext dsl = getDslContext(ctx)) {
            AuthDao auth = new AuthDao(dsl,null);
            ApiKey sourceData = ctx.bodyAsClass(ApiKey.class);
            ApiKey key = auth.createApiKey(p, sourceData);
            if(key == null) {
                ctx.status(HttpCode.BAD_REQUEST);
            } else {
                ctx.json(key).status(HttpCode.CREATED);
            }
        }
    }

    @Override
    public void delete(Context ctx, String arg1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    @OpenApi(
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey[].class, type = Formats.JSON)
                    },
                    status = "201"
        ),
        description = "View all keys for the current user",
        tags = {"Authorization"}
    )
    public void getAll(Context ctx) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        try(DSLContext dsl = getDslContext(ctx)) {
            AuthDao auth = new AuthDao(dsl,null);
            List<ApiKey> keys = auth.apiKeysForUser(p);
            ctx.json(keys).status(HttpCode.OK);            
        }
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name="key-name", required = true, description = "Name of the specific key to get more information for. NOTE: Case-sensitive.")
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey.class, type = Formats.JSON)
                    },
                    status = "201"
        ),
        description = "View specific key",
        tags = {"Authorization"}
    )
    @Override
    public void getOne(Context ctx, String keyName) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        try(DSLContext dsl = getDslContext(ctx)) {
            AuthDao auth = new AuthDao(dsl,null);
            ApiKey key = auth.apiKeyForUser(p,keyName);
            if(key != null) {
                ctx.json(key).status(HttpCode.OK);            
            } else {
                throw new NotFoundException("Requested Key was not found. NOTE: api key names are case-sensitive.");
            }
            
        }
    }

    @Override
    public void update(Context ctx, String arg1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'update'");
    }
    
}
