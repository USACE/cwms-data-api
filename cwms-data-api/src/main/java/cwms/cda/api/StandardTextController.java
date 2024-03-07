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

package cwms.cda.api;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.texttimeseries.StandardTextDao;
import cwms.cda.data.dto.texttimeseries.StandardTextCatalog;
import cwms.cda.data.dto.texttimeseries.StandardTextValue;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;


public class StandardTextController implements CrudHandler {
    private static final String TAG = "Standard Text";
    private final MetricRegistry metrics;

    public StandardTextController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected StandardTextDao getDao(DSLContext dsl) {
        return new StandardTextDao(dsl);
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            description = "Retrieve a Standard Text catalog",
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description = "Specifies the office filter of the"
                            + "standard text."),
                    @OpenApiParam(name = STANDARD_TEXT_ID_MASK, description = "Specifies the text id filter of the "
                            + "standard text")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = StandardTextCatalog.class)
                            }
                    )},
            method = HttpMethod.GET,
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            String officeMask = ctx.queryParam(OFFICE_MASK);
            if (officeMask == null) {
                officeMask = "*";
            }
            String idMask = ctx.queryParam(NAME_MASK);
            if (idMask == null) {
                idMask = "*";
            }
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
            DSLContext dsl = getDslContext(ctx);
            StandardTextCatalog catalog = getDao(dsl).retreiveStandardTextCatalog(idMask, officeMask);

            ctx.contentType(contentType.toString());
            String result = Formats.format(contentType, catalog);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }


    @OpenApi(
            description = "Retrieve a single Standard Text value",
            pathParams = {
                    @OpenApiParam(name = STANDARD_TEXT_ID, description = "Specifies the text id of the " +
                            "standard text to retrieve. Default includes all text ids"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of the"
                            + "standard text. Default includes all offices")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = StandardTextValue.class)
                            }
                    )},
            method = HttpMethod.GET,
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String stdTextId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String office = ctx.queryParam(OFFICE);
            if (office == null) {
                throw new IllegalArgumentException(OFFICE + " is a required parameter");
            }
            DSLContext dsl = getDslContext(ctx);
            StandardTextValue standardTextValue = getDao(dsl).retrieveStandardText(stdTextId, office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
            ctx.contentType(contentType.toString());
            String result = Formats.format(contentType, standardTextValue);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
            description = "Create new Standard Text",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = StandardTextValue.class, type = Formats.JSONV2)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            String body = ctx.body();

            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(false);
            StandardTextValue tts = deserialize(body, formatHeader);
            DSLContext dsl = getDslContext(ctx);
            getDao(dsl).storeStandardText(tts.getId().getId(), tts.getStandardText(), tts.getId().getOfficeId(),
                    failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldTextTimeSeriesId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }


    @OpenApi(
            description = "Delete a single Standard Text value",
            pathParams = {
                    @OpenApiParam(name = STANDARD_TEXT_ID, description = "Specifies the text id of the standard " +
                            "text to delete"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of the"
                            + "standard text."),
                    @OpenApiParam(name = METHOD, required = true, description = "Specifies the delete method used.",
                            type = JooqDao.DeleteMethod.class)
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String stdTextId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String office = ctx.queryParam(OFFICE);
            if (office == null) {
                throw new IllegalArgumentException(OFFICE + " is a required parameter");
            }
            JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                    .getOrThrow(e -> new IllegalArgumentException(METHOD + " is a required parameter"));
            String deleteAction;
            switch (deleteMethod) {
                case DELETE_ALL:
                    deleteAction = DeleteRule.DELETE_ALL.getRule();
                    break;
                case DELETE_DATA:
                    deleteAction = DeleteRule.DELETE_DATA.getRule();
                    break;
                case DELETE_KEY:
                    deleteAction = DeleteRule.DELETE_KEY.getRule();
                    break;
                default:
                    throw new IllegalArgumentException("Delete Method provided does not match accepted rule constants: "
                            + deleteMethod);
            }
            DSLContext dsl = getDslContext(ctx);
            getDao(dsl).deleteStandardText(stdTextId, office, deleteAction);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    private static StandardTextValue deserialize(String body, String format) throws JsonProcessingException {
        StandardTextValue retval;
        if (ContentType.equivalent(Formats.JSONV2, format)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, StandardTextValue.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        return retval;
    }

}
