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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.JsonRatingUtils;
import cwms.radar.data.dao.RatingSpecDao;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.RatingSpecs;
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


public class RatingSpecController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingSpecController.class.getName());
    private static final String TAG = "Ratings";

    private final MetricRegistry metrics;

    private static final int DEFAULT_PAGE_SIZE = 100;

    private final Histogram requestResultSize;

    public RatingSpecController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the Rating Specs whose data is to be included in the response. If "
                            + "this field is not specified, matching rating information from all "
                            + "offices shall be returned."),
                    @OpenApiParam(name = "rating-id-mask", description = "RegExp that specifies "
                            + "the rating IDs to be included in the response. If this field is "
                            + "not specified, all Rating Specs shall be returned."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                            description = "How many entries per page returned. "
                            + "Default " + DEFAULT_PAGE_SIZE + "."
                    ),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = RatingSpecs.class)
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
        String ratingIdMask = ctx.queryParam("rating-id-mask");

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            RatingSpecDao ratingSpecDao = getRatingSpecDao(dsl);
            RatingSpecs ratingSpecs = ratingSpecDao.retrieveRatingSpecs(cursor, pageSize, office,
                    ratingIdMask);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, ratingSpecs);
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

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = RATING_ID, required = true, description = "Specifies "
                            + "the rating-id of the Rating Spec to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the Rating Specs whose data is to be included in "
                            + "the response. If this field is not specified, matching rating "
                            + "information from all offices shall be returned."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = RatingSpec.class, type = Formats.JSONV2),
                            }
                    )
            },
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String ratingId) {
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            RatingSpecDao ratingSpecDao = getRatingSpecDao(dsl);

            Optional<RatingSpec> template = ratingSpecDao.retrieveRatingSpec(office, ratingId);
            if (template.isPresent()) {
                String result = Formats.format(contentType, template.get());

                ctx.result(result);
                ctx.contentType(contentType.toString());

                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find Rating Spec based on parameters "
                        + "given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }

    @NotNull
    protected RatingSpecDao getRatingSpecDao(DSLContext dsl) {
        return new RatingSpecDao(dsl);
    }


    @OpenApi(
        description = "Create new Rating Specification",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = RatingSpec.class, type = Formats.XMLV2)
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
            RatingSpecDao dao = new RatingSpecDao(dsl);
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
        // generated methods, choose Tools | Specs.
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = RATING_ID, required = true, description = "The rating-spec-id of the ratings data to be deleted."),
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
    public void delete(Context ctx, @NotNull String ratingSpecId) {
        try (final Timer.Context ignored = markAndTime(DELETE);
             DSLContext dsl = getDslContext(ctx)) {
            String office = ctx.queryParam(OFFICE);
            RatingSpecDao ratingDao = getRatingSpecDao(dsl);
            JooqDao.DeleteMethod method = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class).get();
            ratingDao.delete(office, method, ratingSpecId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

}
