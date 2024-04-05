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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.Tsv;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;

public class TimeSeriesRecentController implements Handler {
    private static final Logger logger = Logger.getLogger(TimeSeriesRecentController.class.getName());
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public TimeSeriesRecentController(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((name(TimeSeriesRecentController.class, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    private DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @NotNull
    private TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
        return new TimeSeriesDaoImpl(dsl);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                        + "timeseries group(s) whose data is to be included in the response. "
                        + "If this field is not specified, matching timeseries groups "
                        + "information from all offices shall be returned."),
                @OpenApiParam(name = CATEGORY_ID, description = "Specifies the category id "
                        + "of the timeseries to be included in the response.  Optional."),
                @OpenApiParam(name = GROUP_ID, description = "Specifies the group id "
                        + "of the timeseries to be included in the response.  Optional."),
                @OpenApiParam(name = TS_IDS, description = "Accepts a comma separated list of "
                        + "timeseries ids to be included in the response.  Optional. "
                        + "Cannot be used in combination with category_id and group_id."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(isArray = true, from = Tsv.class, type = Formats.JSON)}),
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                        + "inputs provided the timeseries group(s) were not found."),
                @OpenApiResponse(status = STATUS_501, description = "request format is not "
                        + "implemented")
            },
            path = "/timeseries/recent",
            description = "Returns CWMS Timeseries Groups Data",
            tags = TimeSeriesController.TAG,
            method = HttpMethod.GET
    )
    public void handle(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime("getRecent")) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParamAsClass(CATEGORY_ID, String.class).allowNullable().get();
            String groupId = ctx.pathParamAsClass(GROUP_ID, String.class).allowNullable().get();
            String tsIdsParam = ctx.queryParamAsClass(TS_IDS, String.class).allowNullable().get();

            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            gregorianCalendar.set(Calendar.HOUR, 0);
            gregorianCalendar.set(Calendar.MINUTE, 0);
            gregorianCalendar.set(Calendar.SECOND, 0);
            gregorianCalendar.set(Calendar.MILLISECOND, 0);

            gregorianCalendar.add(Calendar.HOUR, 24 * 14);
            Timestamp futureLimit = Timestamp.from(gregorianCalendar.toInstant());
            gregorianCalendar.add(Calendar.HOUR, 24 * -28);
            Timestamp pastLimit = Timestamp.from(gregorianCalendar.toInstant());

            boolean hasTsGroupInfo = categoryId != null && !categoryId.isEmpty()
                    || groupId != null && !groupId.isEmpty();
            List<String> tsIds = getTsIds(tsIdsParam);
            boolean hasTsIds = tsIds != null && !tsIds.isEmpty();

            List<RecentValue> latestValues;
            if (hasTsGroupInfo && hasTsIds) {
                // has both = this is an error
                CdaError re = new CdaError("Invalid arguments supplied, group has both "
                        + "Timeseries Group info and Timeseries IDs.");
                logger.log(Level.SEVERE, "{0} for request {1}", new Object[]{ re, ctx.fullUrl()});
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if (!hasTsGroupInfo && !hasTsIds) {
                // doesn't have either?  Just return empty results?
                CdaError re = new CdaError("Invalid arguments supplied, group has neither "
                        + "Timeseries Group info nor Timeseries IDs");
                logger.log(Level.SEVERE, "{0} for request {1}", new Object[]{ re, ctx.fullUrl()});
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.json(re);
                return;
            } else if (hasTsGroupInfo) {
                // just group provided
                latestValues = dao.findRecentsInRange(office, categoryId, groupId, pastLimit, futureLimit);
            } else {
                latestValues = dao.findMostRecentsInRange(tsIds, pastLimit, futureLimit);
            }

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = Formats.format(contentType, latestValues, RecentValue.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    public static List<String> getTsIds(String tsIdsParam) {
        List<String> retval = null;

        if (tsIdsParam != null && !tsIdsParam.isEmpty()) {
            retval = new ArrayList<>();

            if (tsIdsParam.startsWith("[")) {
                tsIdsParam = tsIdsParam.substring(1);
            }

            if (tsIdsParam.endsWith("]")) {
                tsIdsParam = tsIdsParam.substring(0, tsIdsParam.length() - 1);
            }

            if (!tsIdsParam.isEmpty()) {
                final String regex = "\"[^\"]*\"|[^,]+";
                final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

                try (Scanner s = new Scanner(tsIdsParam)) {
                    List<String> matches = findAll(s, pattern).map(m -> m.group().trim())
                            .collect(Collectors.toList());
                    retval.addAll(matches);
                }
            }
        }

        return retval;
    }

    // This came from https://stackoverflow.com/questions/42961296/java-8-stream-emitting-a-stream/42978216#42978216
    private static Stream<MatchResult> findAll(Scanner s, Pattern pattern) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<MatchResult>(
                1000, Spliterator.ORDERED | Spliterator.NONNULL) {
            public boolean tryAdvance(Consumer<? super MatchResult> action) {
                if (s.findWithinHorizon(pattern, 0) != null) {
                    action.accept(s.match());
                    return true;
                } else {
                    return false;
                }
            }
        }, false);
    }
}
