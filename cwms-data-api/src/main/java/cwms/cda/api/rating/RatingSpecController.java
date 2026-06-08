/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.api.rating;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.RatingSpecDao;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.data.dto.rating.RatingSpecs;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
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

import com.google.common.flogger.FluentLogger;

import javax.servlet.http.HttpServletResponse;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class RatingSpecController implements CrudHandler {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final String TAG = "Ratings";

    private final MetricRegistry metrics;

    static final int DEFAULT_PAGE_SIZE = 100;

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
                    @OpenApiParam(name = RATING_ID_MASK, description = "Posix "
                            + "<a href=\"regexp.html\">regular expression</a>  that specifies "
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
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = RatingSpecs.class),
                                    @OpenApiContent(type = Formats.XMLV2, from = RatingSpecs.class)
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
        String ratingIdMask = ctx.queryParam(RATING_ID_MASK);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, RatingSpecs.class);
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            RatingSpecDao ratingSpecDao = getRatingSpecDao(dsl);
            RatingSpecs ratingSpecs = ratingSpecDao.retrieveRatingSpecs(cursor, pageSize, office,
                    ratingIdMask);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, ratingSpecs);
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);

            byte[] bytes = result.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException e) {
            CdaError re = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Ratings", e);
            logger.atInfo().log("%s%sfor request %s", re, System.lineSeparator(), ctx.fullUrl());
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = RATING_ID, required = true, description = "Specifies "
                            + "the rating-id of the Rating Spec to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the "
                            + "owning office of the Rating Specs whose data is to be included in "
                            + "the response. If this field is not specified, matching rating "
                            + "information from all offices shall be returned."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = RatingSpec.class, type = Formats.JSONV2),
                                    @OpenApiContent(from = RatingSpec.class, type = Formats.XMLV2)
                            }
                    )
            },
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, @NotNull String ratingId) {
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, RatingSpec.class);

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            RatingSpecDao ratingSpecDao = getRatingSpecDao(dsl);

            Optional<RatingSpec> template = ratingSpecDao.retrieveRatingSpec(office, ratingId);
            if (template.isPresent()) {
                String result = Formats.format(contentType, template.get());

                ctx.contentType(contentType.toString());

                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);

                byte[] bytes = result.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            } else {
                CdaError re = new CdaError("Unable to find Rating Spec based on parameters "
                        + "given");
                logger.atInfo().log("%s%sfor request %s", re, System.lineSeparator(), ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        } catch (IOException e) {
            CdaError re = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Ratings", e);
            logger.atInfo().log("%s%sfor request %s", re, System.lineSeparator(), ctx.fullUrl());
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
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
                            @OpenApiContent(from = RatingSpec.class, type = Formats.JSON),
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
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String contentTypeHeader = ctx.req.getContentType();
            String body = ctx.body();
            ContentType contentType = Formats.parseHeader(contentTypeHeader, RatingSpec.class);

            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(false);
            RatingSpecDao dao = new RatingSpecDao(dsl);

            try {
                RatingSpec spec = Formats.parseContent(contentType, body, RatingSpec.class);
                // If we can parse it into our CDA RatingSpec object have the DAO use it.
                dao.create(spec, failIfExists);
                ctx.status(HttpServletResponse.SC_CREATED);
            } catch (FormattingException e) {
                if (contentType.getType().contains(Formats.XML)) {
                    // The user said its xml but it doesn't parse into our CDA RatingSpec object.
                    // We'll let the dao try doing a string pass-thru to the pl/sql.
                    dao.create(body, failIfExists);
                    ctx.status(HttpServletResponse.SC_CREATED);
                    return;
                }
                throw e;
            }
        }
    }


    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, @NotNull String locationCode) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = RATING_ID, required = true, description = "The rating-spec-id of the ratings data to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the ratings to be deleted."),
                    @OpenApiParam(name = METHOD, required = true, description = "Specifies the delete method used.",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Deletes requested rating specification",
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String ratingSpecId) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);

            String office = requiredParam(ctx, OFFICE);
            RatingSpecDao ratingDao = getRatingSpecDao(dsl);
            JooqDao.DeleteMethod method = requiredParamAs(ctx, METHOD, JooqDao.DeleteMethod.class);
            ratingDao.delete(office, method, ratingSpecId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

}
