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
import static cwms.radar.api.Controllers.*;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.JsonRatingUtils;
import cwms.radar.data.dao.RatingTemplateDao;
import cwms.radar.data.dto.rating.RatingTemplate;
import cwms.radar.data.dto.rating.RatingTemplates;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.TransformerException;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class RatingTemplateController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingTemplateController.class.getName());

    private static final String TAG = "Ratings";
    private final MetricRegistry metrics;

    private static final int DEFAULT_PAGE_SIZE = 100;

    private final Histogram requestResultSize;

    public RatingTemplateController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the Rating Templates whose data is to be included in the response."
                            + " If this field is not specified, matching rating information from "
                            + "all offices shall be returned."),
                    @OpenApiParam(name = TEMPLATE_ID_MASK, description = "RegExp that specifies"
                            + " the rating template IDs to be included in the response. If this "
                            + "field is not specified, all rating templates shall be returned."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entries per page returned. "
                                    + "Default " + DEFAULT_PAGE_SIZE + "."
                    ),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = RatingTemplates.class, type =
                                            Formats.JSONV2),
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String cursor = ctx.queryParamAsClass(PAGE, String.class).getOrDefault("");
        int pageSize =
                ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(DEFAULT_PAGE_SIZE);

        String office = ctx.queryParam(OFFICE);
        String templateIdMask = ctx.queryParam(TEMPLATE_ID_MASK);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            RatingTemplateDao ratingTemplateDao = getRatingTemplateDao(dsl);
            RatingTemplates ratingTemplates = ratingTemplateDao.retrieveRatingTemplates(cursor,
                    pageSize, office,
                    templateIdMask);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, ratingTemplates);
            ctx.result(result);
            requestResultSize.update(result.length());
        } catch (Exception ex) {
            RadarError re =
                    new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @NotNull
    private RatingTemplateDao getRatingTemplateDao(DSLContext dsl) {
        return new RatingTemplateDao(dsl);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TEMPLATE_ID, required = true, description = "Specifies"
                            + " the template whose data is to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the Rating Templates whose data is to be included"
                            + " in the response. If this field is not specified, matching rating "
                            + "information from all offices shall be returned."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(isArray = true, from = RatingTemplate.class,
                                            type = Formats.JSONV2),
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String templateId) {
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            RatingTemplateDao ratingSetDao = getRatingTemplateDao(dsl);

            Optional<RatingTemplate> template = ratingSetDao.retrieveRatingTemplate(office,
                    templateId);
            if (template.isPresent()) {
                String result = Formats.format(contentType, template.get());

                ctx.result(result);
                ctx.contentType(contentType.toString());

                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find Rating Template based on "
                        + "parameters given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }


    @OpenApi(
        description = "Create new Rating Template",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = RatingTemplate.class, type = Formats.XMLV2)
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
    public void create(Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE);
             DSLContext dsl = getDslContext(ctx)) {
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.XMLV2;
            String body = ctx.body();
            String xml = translateToXml(body, formatHeader);
            RatingTemplateDao dao = new RatingTemplateDao(dsl);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(false);
            dao.create(xml, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    private static String translateToXml(String body, String contentType) {
        String retval;


        if (contentType.contains(Formats.XMLV2)) {
            retval = body;
        } else if (contentType.contains(Formats.JSONV2)) {
            retval = translateJsonToXml(body);
        } else {
            throw new IllegalArgumentException("Unexpected contentType format:" + contentType);
        }

        return retval;
    }

    private static String translateJsonToXml(String body) {
        String retval;
        try {
            retval = JsonRatingUtils.jsonToXml(body);
        } catch (IOException | TransformerException ex) {
            throw new IllegalArgumentException("Failed to translate request into rating spec XML", ex);
        }
        return retval;
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of
        // generated methods, choose Tools | Templates.
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = TEMPLATE_ID, required = true, description = "The rating-template-id of the ratings data to be deleted."),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the ratings to be deleted."),
            @OpenApiParam(name = METHOD,  required = true, description = "Specifies the delete method used.",
                type = JooqDao.DeleteMethod.class)
        },
        description = "Deletes requested rating specification",
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(Context ctx, String ratingTemplateId) {
        try (final Timer.Context ignored = markAndTime(DELETE);
             DSLContext dsl = getDslContext(ctx)) {
            String office = ctx.queryParam(OFFICE);
            RatingTemplateDao ratingDao = new RatingTemplateDao(dsl);
            JooqDao.DeleteMethod method = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class).get();
            ratingDao.delete(office, method, ratingTemplateId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

}
