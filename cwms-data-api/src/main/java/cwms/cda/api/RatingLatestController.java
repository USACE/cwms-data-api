/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RATING_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.RatingDao;
import cwms.cda.data.dao.RatingSetDao;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class RatingLatestController implements Handler {
    private static final String TAG = "Ratings";
    private final MetricRegistry metrics;

    public RatingLatestController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @NotNull
    protected RatingDao getRatingDao(DSLContext dsl) {
        return new RatingSetDao(dsl);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = RATING_ID, required = true, description = "The rating-id of the effective "
                    + "dates to be retrieve. "),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description =
                "Specifies the owning office of the ratingset to be included in the "
                + "response."),
            @OpenApiParam(name = METHOD, description = "Specifies "
                + "the retrieval method used.  If no method is provided EAGER will be used.",
                type = RatingSet.DatabaseLoadMethod.class),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(type = Formats.JSONV2),
                @OpenApiContent(type = Formats.XMLV2)})
        },
        description = "Returns CWMS Rating Data",
        tags = {TAG})
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String rating = ctx.pathParam(RATING_ID);

            ContentType contentType = new ContentType(ctx.contentType() != null ? ctx.contentType() : Formats.JSONV2);

            String officeId = ctx.queryParam(OFFICE);

            RatingSet.DatabaseLoadMethod method = ctx.queryParamAsClass(METHOD,
                            RatingSet.DatabaseLoadMethod.class)
                    .getOrDefault(RatingSet.DatabaseLoadMethod.EAGER);

            if (!contentType.toString().equals(Formats.JSONV2) && !contentType.toString().equals(Formats.XMLV2)) {
                ctx.status(HttpCode.UNSUPPORTED_MEDIA_TYPE);
            }

            String body = getLatestRatingSet(ctx, method, officeId, rating, contentType);
            ctx.contentType(contentType.toString());
            if (body != null) {
                ctx.result(body);
                ctx.status(HttpCode.OK);
            } else {
                ctx.status(HttpCode.NOT_FOUND);
            }
        }
    }

    private String getLatestRatingSet(Context ctx, RatingSet.DatabaseLoadMethod method,
            String officeId, String rating, ContentType contentType)
            throws RatingException {
        String ratingSet = null;
        try (final Timer.Context ignored = markAndTime("getLatestRatingSet")) {
            DSLContext dsl = getDslContext(ctx);

            RatingDao ratingDao = getRatingDao(dsl);

            if (contentType.toString().equals(Formats.JSONV2)) {
                ratingSet = ratingDao.retrieveLatestJSON(method, officeId, rating);

            } else if (contentType.toString().equals(Formats.XMLV2)) {
                ratingSet = ratingDao.retrieveLatestXML(officeId, rating);
            } else {

                return null;
            }
        }

        return ratingSet;
    }
}
