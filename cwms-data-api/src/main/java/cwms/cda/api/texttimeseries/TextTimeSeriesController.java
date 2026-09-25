/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda.api.texttimeseries;

import static cwms.cda.api.Controllers.DATE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.ReplaceUtils;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public abstract class TextTimeSeriesController extends BaseCrudHandler {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String TAG = "Text-TimeSeries";

    public static final String REPLACE_ALL = "replace-all";

    public static final boolean DEFAULT_CREATE_REPLACE_ALL = false;
    public static final boolean DEFAULT_UPDATE_REPLACE_ALL = true;

    protected TextTimeSeriesController(com.codahale.metrics.MetricRegistry metrics) {
        super(metrics);
    }

    @NotNull
    protected TimeSeriesTextDao getDao(DSLContext dsl) {
        return new TimeSeriesTextDao(dsl);
    }

    protected abstract String getOffice(@NotNull Context ctx);

    @Override
    public void getAll(@NotNull Context ctx) {

        String office = getOffice(ctx);
        String tsId = requiredParam(ctx, NAME);
        Instant begin = requiredInstant(ctx, Controllers.BEGIN);
        Instant end = requiredInstant(ctx, Controllers.END);
        Instant version = Controllers.queryParamAsInstant(ctx, VERSION_DATE);
        int kiloByteLimit = Integer.parseInt(System.getProperty("cda.api.ts.text.max.length.kB", "64"));
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, TextTimeSeries.class);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesTextDao dao = getDao(dsl);

            String textMask = "*";

            String dateToken = "{date_token}";
            String path = ctx.path();
            if (!path.endsWith("/"))  {
                path += "/";
            }
            path += tsId + "/value";
            String url = new URIBuilder(ctx.fullUrl())
                    .setPath(path)
                    .clearParameters()
                    .addParameter(OFFICE, office)
                    .addParameter(VERSION_DATE, ctx.queryParam(VERSION_DATE))
                    .addParameter(DATE, dateToken)
                    .build()
                    .toString();
            ReplaceUtils.OperatorBuilder urlBuilder = new ReplaceUtils.OperatorBuilder()
                    .withTemplate(url)
                    .withOperatorKey(URLEncoder.encode(dateToken, "UTF-8"));
            TextTimeSeries textTimeSeries = dao.retrieveFromDao(office, tsId, textMask,
                    begin, end, version, kiloByteLimit, urlBuilder);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, textTimeSeries);

            ctx.status(HttpServletResponse.SC_OK);

            byte[] bytes = result.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (URISyntaxException | IOException ex) {
            CdaError re = ExceptionTraceSupport.buildError(ctx,
                    "Failed to process request: " + ex.getLocalizedMessage(), ex);
            logger.atSevere().withCause(ex).log("%s", re);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String templateId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(Controllers.CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();

            ContentType contentType = Formats.parseHeader(formatHeader, TextTimeSeries.class);
            TextTimeSeries tts = Formats.parseContent(contentType, ctx.bodyAsInputStream(), TextTimeSeries.class);
            TimeSeriesTextDao dao = getDao(dsl);

            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class)
                    .getOrDefault(DEFAULT_CREATE_REPLACE_ALL);
            dao.create(tts, replaceAll);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @Override
    public void delete(@NotNull Context ctx, @NotNull String textTimeSeriesId) {
        try (Timer.Context ignored = markAndTime(Controllers.DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = getOffice(ctx);
            String mask = requiredParam(ctx, Controllers.TEXT_MASK);


            Instant begin = requiredInstant(ctx, Controllers.BEGIN);
            Instant end = requiredInstant(ctx, Controllers.END);
            Instant version = Controllers.queryParamAsInstant(ctx, VERSION_DATE);

            TimeSeriesTextDao dao2 = getDao(dsl);

            dao2.delete(office, textTimeSeriesId, mask, begin, end, version);

            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
