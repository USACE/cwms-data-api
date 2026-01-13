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

package cwms.cda.api.rss;

import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.SINCE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.BaseHandler;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.rss.MessageDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.rss.RssFeed;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.ReplaceUtils;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.UnaryOperator;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class RssHandler extends BaseHandler {

    private static final int DEFAULT_PAGE_SIZE = 500;
    private static final String TAG = "RSS";

    public RssHandler(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Office id for feed."),
            @OpenApiParam(name = NAME, required = true, description = "Specifies the name of the feed. " +
                "eg TS_STORED, STATUS, REALTIME_OPS")
        },
        queryParams = {
            @OpenApiParam(name = SINCE, description = "The start the feed time window. " +
                "The endpoint will not retrieve more than the last week of messages."),
            @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "The number of feed items to include."),
            @OpenApiParam(name = PAGE, description = "This end point can return a lot of data, this "
                + "identifies where in the request you are. This is an opaque"
                + " value, and can be obtained from the 'next-page' value in "
                + "the response.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(from = RssFeed.class, type = Formats.RSS)
            }),
            @OpenApiResponse(status = STATUS_404, description = "Unknown Feed")
        },
        description = "Returns RSS feed items limited to the last week.",
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.pathParam(OFFICE).toUpperCase();
            String name = ctx.pathParam(NAME);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, RssFeed.class);
            String cursor = URLDecoder.decode(queryParamAsClass(ctx, new String[]{PAGE, CURSOR}, String.class, ""),
                StandardCharsets.UTF_8);
            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                    .status(HttpCode.BAD_REQUEST);
                return;
            }
            Instant since = queryParamAsInstant(ctx, SINCE);
            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE}, Integer.class, DEFAULT_PAGE_SIZE);
            MessageDao dao = new MessageDao(dsl);
            RssFeed feed = dao.retrieveFeed(cursor, pageSize, office, name, since, newLinkTemplate(ctx));
            String result = Formats.format(contentType, feed);
            ctx.result(result);
            ctx.contentType(contentType.toString());
        }
    }

    private static String getHost(Context ctx) {
        String scheme = ctx.header("X-Forwarded-Proto");
        if (scheme == null) {
            scheme = "https";
        }
        String host = ctx.header("X-Forwarded-Host");
        if (host == null) {
            host = ctx.host();
        }
        String path = ctx.path();
        return scheme + "://" + host + path;
    }

    private UnaryOperator<String> newLinkTemplate(Context ctx)
        throws URISyntaxException {
        String pageToken = "{page_token}";
        String url = new URIBuilder(getHost(ctx))
            .addParameter(PAGE, pageToken)
            .build()
            .toString();
        return new ReplaceUtils.OperatorBuilder()
            .withTemplate(url)
            .withOperatorKey(URLEncoder.encode(pageToken, StandardCharsets.UTF_8))
            .build();
    }
}
