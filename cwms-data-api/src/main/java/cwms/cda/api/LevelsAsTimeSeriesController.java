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
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DateUtils;
import hec.data.level.JDomLocationLevelRef;
import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class LevelsAsTimeSeriesController implements Handler {
    private final MetricRegistry metrics;

    public LevelsAsTimeSeriesController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies"
                            + " the requested location level."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "office of the Location Level to be returned"),
                    @OpenApiParam(name = INTERVAL, description = "Interval time step for"
                            + " the returned time series. Pseudo-regular interval definitions"
                            + " will be treated like local regular. Irregular interval will generate daily time steps."
                            + " Default: 0"),
                    @OpenApiParam(name = BEGIN, description = "Specifies the "
                            + "start of the time window for data to be included in the response. "
                            + "If this field is not specified, any required time window begins 24"
                            + " hours prior to the specified or default end time. The format for "
                            + "this field is ISO 8601 extended, with optional offset and "
                            + "timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = END,  description = "Specifies the "
                            + "end of the time window for data to be included in the response. If"
                            + " this field is not specified, any required time window ends at the"
                            + " current time. The format for this field is ISO 8601 extended, "
                            + "with optional timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified), as well as the time zone of any times in the"
                            + " response. If this field is not specified, the default time zone "
                            + "of UTC shall be used.\r\nIgnored if begin was specified with "
                            + "offset and timezone."),
                    @OpenApiParam(name = UNIT, required = true, description = "Desired unit for "
                            + "the values retrieved."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            description = "A CWMS Time Series representation of the specified location level.",
                            content = {
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.XML),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                                    @OpenApiContent(from = TimeSeries.class, type = ""),
                            }
                    ),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a timeseries."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            description = "Retrieves requested Location Level",
            tags = LevelsController.TAG
    )
    public void handle(Context ctx) {

        try (final Timer.Context timeContext = markAndTime("getLevelAsTimeSeries")) {
            DSLContext dsl = getDslContext(ctx);
            Validator<String> pathParam = ctx.pathParamAsClass(LEVEL_ID, String.class);
            if (!pathParam.hasValue()) {
                throw new IllegalArgumentException(LEVEL_ID + " path parameter can not be null when retrieving levels as time series");
            }
            String levelId = pathParam.get();
            String office = requiredParam(ctx, OFFICE);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);
            String units = requiredParam(ctx, UNIT);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String intervalParameter = ctx.queryParamAsClass(INTERVAL, String.class).getOrDefault("0");

            ZoneId tz = ZoneId.of(timezone, ZoneId.SHORT_IDS);
            begin = begin != null ? begin : "PT-24H";

            ZonedDateTime beginZdt = DateUtils.parseUserDate(begin, timezone);
            ZonedDateTime endZdt = end != null
                    ? DateUtils.parseUserDate(end, timezone)
                    : ZonedDateTime.now(tz);

            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(dsl);
            Interval interval = IntervalFactory.findAny(IntervalFactory.equalsName(intervalParameter))
                    .orElseThrow(() -> new IllegalArgumentException("Invalid interval string: " + intervalParameter + " for location level as timeseries"));
            JDomLocationLevelRef levelRef = new JDomLocationLevelRef(office, levelId);
            TimeSeries timeSeries = levelsDao.retrieveLocationLevelAsTimeSeries(levelRef, beginZdt.toInstant(), endZdt.toInstant(), interval, units);
            ctx.json(timeSeries);
            ctx.status(HttpServletResponse.SC_OK);
        }

    }
}
