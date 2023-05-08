/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.radar.api.Controllers.FAIL_IF_EXISTS;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.api.Controllers.SPECIFIED_LEVEL_ID;
import static cwms.radar.api.Controllers.TEMPLATE_ID_MASK;
import static cwms.radar.api.Controllers.UPDATE;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.SpecifiedLevelDao;
import cwms.radar.data.dto.SpecifiedLevel;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class SpecifiedLevelController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(SpecifiedLevelController.class.getName());
    private static final String TAG = "Specified Levels";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public SpecifiedLevelController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    @NotNull
    protected SpecifiedLevelDao getDao(DSLContext dsl) {
        return new SpecifiedLevelDao(dsl);
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the Specified Levels whose data is to be included in the response."
                            + " If this field is not specified, matching rating information from "
                            + "all offices shall be returned."),
                    @OpenApiParam(name = TEMPLATE_ID_MASK, description = "Mask that specifies "
                            + "the IDs to be included in the response. If this field is not "
                            + "specified, all specified levels shall be returned."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from =
                                            SpecifiedLevel.class)
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String office = ctx.queryParam(OFFICE);
        String templateIdMask = ctx.queryParam(TEMPLATE_ID_MASK);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            SpecifiedLevelDao dao = getDao(dsl);
            List<SpecifiedLevel> levels = dao.getSpecifiedLevels(office, templateIdMask);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, levels, SpecifiedLevel.class);
            ctx.result(result);
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        } catch (Exception ex) {
            RadarError re =
                    new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String templateId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET); //To change body of
        // generated methods, choose Tools | Specs.
    }

    @OpenApi(
        description = "Create new SpecifiedLevel",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = SpecifiedLevel.class, type = Formats.JSONV2)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true")
        },
        method = HttpMethod.POST,
        tags = {TAG}
    )
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime("create");
             DSLContext dsl = getDslContext(ctx)) {
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            String body = ctx.body();
            SpecifiedLevel deserialize = deserialize(body, formatHeader);
            SpecifiedLevelDao dao = getDao(dsl);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            dao.create(deserialize, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            RadarError re = new RadarError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
        description = "Renames the requested specified level id",
        pathParams = {
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, description = "The specified level id to be renamed"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the timeseries identifier to be renamed"),
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, description = "The new specified level id.")
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    public void update(Context ctx, String oldSpecifiedLevelId) {
        try (Timer.Context ignored = markAndTime(UPDATE);
             DSLContext dsl = getDslContext(ctx)) {
            SpecifiedLevelDao dao = getDao(dsl);
            String newSpecifiedLevelId = ctx.queryParam(SPECIFIED_LEVEL_ID);
            String office = ctx.queryParam(OFFICE);
            dao.update(oldSpecifiedLevelId, newSpecifiedLevelId, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }



    @OpenApi(
        description = "Deletes requested specified level id",
        pathParams = {
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, description = "The specified level id to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the timeseries identifier to be deleted"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    public void delete(Context ctx, String specifiedLevelId) {
        try (Timer.Context ignored = markAndTime(UPDATE);
             DSLContext dsl = getDslContext(ctx)) {
            SpecifiedLevelDao dao = getDao(dsl);
            String office = ctx.queryParam(OFFICE);
            dao.delete(specifiedLevelId, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    private static SpecifiedLevel deserialize(String body, String format) throws JsonProcessingException {
        SpecifiedLevel retval;
        if (ContentType.equivalent(Formats.JSONV2, format)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, SpecifiedLevel.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        return retval;
    }

}
