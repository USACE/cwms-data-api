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

import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RATING_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.rating.RateController.TAG;
import static cwms.cda.api.rating.RateController.handleRatingDbError;
import static cwms.cda.api.rating.RateController.isRateTimeSeries;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.BaseHandler;
import cwms.cda.data.dao.RateDao;
import cwms.cda.data.dto.rating.RateInput;
import cwms.cda.data.dto.rating.RateInputTimeSeries;
import cwms.cda.data.dto.rating.RateInputValues;
import cwms.cda.data.dto.rating.RatedOutput;
import cwms.cda.data.dto.rating.RatedOutputTimeSeries;
import cwms.cda.data.dto.rating.RatedOutputValues;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

public final class ReverseRateController extends BaseHandler {

    private static final String REVERSE_RATE = "ReverseRate";

    public ReverseRateController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Office owning the rating"),
            @OpenApiParam(name = RATING_ID, description = "Rating Specification identifier"),
        },
        requestBody = @OpenApiRequestBody(content = {
            @OpenApiContent(type = Formats.JSON, from = RateInputValues.class),
            @OpenApiContent(type = Formats.JSONV1, from = RateInputValues.class),
            @OpenApiContent(type = Formats.JSON, from = RateInputTimeSeries.class),
            @OpenApiContent(type = Formats.JSONV1, from = RateInputTimeSeries.class)},
            required = true),
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(type = Formats.JSON, from = RatedOutputTimeSeries.class),
                @OpenApiContent(type = Formats.JSONV1, from = RatedOutputTimeSeries.class),
                @OpenApiContent(type = Formats.JSON, from = RatedOutputValues.class),
                @OpenApiContent(type = Formats.JSONV1, from = RatedOutputValues.class)
            })
        },
        security = {
            @OpenApiSecurity(name = "gets overridden allows lock icon.")
        },
        description = "Returns rated values.",
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (final Timer.Context ignored = markAndTime(REVERSE_RATE)) {
            DSLContext dsl = getDslContext(ctx);
            RateDao ratingDao = new RateDao(dsl);
            String office = ctx.pathParam(OFFICE);
            String ratingId = ctx.pathParam(RATING_ID);
            String contentTypeHeader = ctx.req.getContentType();
            String body = ctx.body();
            String result;
            if(isRateTimeSeries(body)) {
                ContentType contentType = Formats.parseHeader(contentTypeHeader, RateInputTimeSeries.class);
                RateInputTimeSeries input = Formats.parseContent(contentType, body, RateInputTimeSeries.class);
                RatedOutput output = ratingDao.reverseRate(office, ratingId, input);
                String acceptFormatHeader = ctx.header(Header.ACCEPT);
                ContentType acceptContentType = Formats.parseHeader(acceptFormatHeader, RatedOutputTimeSeries.class);
                result = Formats.format(acceptContentType, output);
            } else {
                ContentType contentType = Formats.parseHeader(contentTypeHeader, RateInputValues.class);
                RateInputValues input = Formats.parseContent(contentType, body, RateInputValues.class);
                RatedOutput output = ratingDao.reverseRate(office, ratingId, input);
                String acceptFormatHeader = ctx.header(Header.ACCEPT);
                ContentType acceptContentType = Formats.parseHeader(acceptFormatHeader, RatedOutputValues.class);
                result = Formats.format(acceptContentType, output);
            }
            ctx.status(HttpServletResponse.SC_OK).result(result);
        } catch (DataAccessException ex) {
            handleRatingDbError(ex);
        }
    }
}
