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

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import static cwms.cda.api.Controllers.ALL_UPSTREAM;
import static cwms.cda.api.Controllers.AREA_UNIT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SAME_STREAM_ONLY;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STAGE_UNIT;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import cwms.cda.data.dao.StreamLocationDao;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.*;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class UpstreamLocationsGetController implements Handler {
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public UpstreamLocationsGetController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram(name(className, RESULTS, SIZE));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Office id for the stream location associated with the upstream locations."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the name of the stream location whose upstream locations data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = ALL_UPSTREAM, type = Boolean.class, description = "If true, retrieve all upstream locations."),
                    @OpenApiParam(name = SAME_STREAM_ONLY, type = Boolean.class, description = "If true, retrieve only locations on the same stream."),
                    @OpenApiParam(name = STATION_UNIT, description = "Station units."),
                    @OpenApiParam(name = STAGE_UNIT, description = "Stage units."),
                    @OpenApiParam(name = AREA_UNIT, description = "Area units."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {@OpenApiContent(isArray = true, type = Formats.JSONV1, from = StreamLocation.class)})
            },
            description = "Returns matching upstream stream locations.",
            tags = {StreamLocationController.TAG}
    )
    public void handle(@NotNull Context ctx) throws Exception {
        String locationId =  ctx.pathParam(NAME);
        String office = ctx.pathParam(OFFICE);
        Boolean allUpstream = ctx.queryParamAsClass(ALL_UPSTREAM, Boolean.class).getOrDefault(false);
        Boolean sameStreamOnly = ctx.queryParamAsClass(SAME_STREAM_ONLY, Boolean.class).getOrDefault(false);
        String stationUnits = ctx.queryParam(STATION_UNIT);
        String stageUnits = ctx.queryParam(STAGE_UNIT);
        String areaUnits = ctx.queryParam(AREA_UNIT);

        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            List<StreamLocation> upstreamLocations = dao.retrieveUpstreamLocations(office, locationId, allUpstream, sameStreamOnly, stationUnits, stageUnits, areaUnits);

            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamLocation.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, upstreamLocations, StreamLocation.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }
}
