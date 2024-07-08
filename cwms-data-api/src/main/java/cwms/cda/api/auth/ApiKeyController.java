/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.auth;

import static cwms.cda.api.Controllers.STATUS_201;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.formatters.Formats;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class ApiKeyController implements CrudHandler {
    public final MetricRegistry metrics;


    public ApiKeyController(MetricRegistry metrics) {
        this.metrics = metrics;

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
                    status = STATUS_201
        ),
        description = "Create a new API Key for user. The randomly generated key is returned "
                + "to the caller. A provided key will be ignored.",
        tags = {"Authorization"}
    )
    @Override
    public void create(Context ctx) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        try {
            DSLContext dsl = getDslContext(ctx);
            AuthDao auth = AuthDao.getInstance(dsl);
            ApiKey sourceData = ctx.bodyAsClass(ApiKey.class);
            ApiKey key = auth.createApiKey(p, sourceData);
            if (key == null) {
                ctx.status(HttpCode.BAD_REQUEST);
            } else {
                ctx.json(key).status(HttpCode.CREATED);
            }
        } catch (CwmsAuthException ex) {
            if (ex.getMessage().equals(AuthDao.ONLY_OWN_KEY_MESSAGE)) {
                ctx.json(new CdaError(ex.getMessage(), true)).status(ex.getAuthFailCode());
            } else {
                throw ex;
            }
        }
    }

    @OpenApi(
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey.class, type = Formats.JSON)
                    },
                    status = STATUS_201
        ),
        description = "Delete API key for a user",
        tags = {"Authorization"}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String keyName) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        DSLContext dsl = getDslContext(ctx);

        AuthDao auth = AuthDao.getInstance(dsl);
        auth.deleteKeyForUser(p, keyName);
        ctx.status(HttpCode.NO_CONTENT);
    }

    @OpenApi(
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey[].class, type = Formats.JSON)
                    },
                    status = STATUS_201
        ),
        description = "View all keys for the current user",
        tags = {"Authorization"}
    )
    public void getAll(Context ctx) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);

        DSLContext dsl = getDslContext(ctx);

        AuthDao auth = AuthDao.getInstance(dsl);
        List<ApiKey> keys = auth.apiKeysForUser(p);
        ctx.json(keys).status(HttpCode.OK);

    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = "key-name", required = true,
                description = "Name of the specific key to get more information for. NOTE: Case-sensitive.")
        },
        responses = @OpenApiResponse(
                    content = {
                        @OpenApiContent(from = ApiKey.class, type = Formats.JSON)
                    },
                    status = STATUS_201
        ),
        description = "View specific key",
        tags = {"Authorization"}
    )
    @Override
    public void getOne(Context ctx, @NotNull String keyName) {
        DataApiPrincipal p = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        DSLContext dsl = getDslContext(ctx);
        AuthDao auth = AuthDao.getInstance(dsl);
        ApiKey key = auth.apiKeyForUser(p, keyName);
        if (key != null) {
            ctx.json(key).status(HttpCode.OK);
        } else {
            CdaError msg = new CdaError(
                    "Requested Key was not found. NOTE: api key names are case-sensitive.",
                    true
            );
            ctx.json(msg).status(HttpCode.NOT_FOUND);
        }

    }

    @OpenApi(
        ignore = true // users should delete and recreate keys. There is nothing to update.
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String arg1) {
        throw new UnsupportedOperationException("Update is not implemented. Delete and create a new key.");
    }
    
}
