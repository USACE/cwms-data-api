/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.NAMES;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.LocationController.getLocationsDao;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dto.CwmsIdLocationKind;
import cwms.cda.data.dto.Location;
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
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class LocationKindController implements Handler {
    public static final String TAG = "REGI";

    private final Histogram requestResultSize;

    public LocationKindController(MetricRegistry metrics) {
        MetricRegistry controllerMetrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = controllerMetrics.histogram((name(className, RESULTS, SIZE)));
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = NAMES, description = "Specifies the name(s) of the "
                + "location(s) whose data is to be included in the response. This parameter is a "
                + "Posix <a href=\"regexp.html\">regular expression</a> matching against the id"),
            @OpenApiParam(name = LOCATION_KIND_LIKE, description = "Specifies the location kind(s) "
                + "whose data is to be included in the response. This parameter is a "
                + "Posix <a href=\"regexp.html\">regular expression</a> matching against the location kind. "
                + "If this field is not specified, all location kinds shall be returned."),
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                + "the location level(s) whose data is to be included in the response"
                + ". If this field is not specified, matching location level "
                + "information from all offices shall be returned."),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(isArray = true, type = Formats.JSONV2, from = Location.class),
                })
        },
        description = "Returns CWMS Location Data.  The Catalog end-point is also capable of "
            + "retrieving lists of locations and can filter on additional fields.",
        method = HttpMethod.GET,
        path = "/locations/with-kind",
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
        DSLContext dsl = getDslContext(ctx);

        LocationsDao locationsDao = getLocationsDao(dsl);

        String names = ctx.queryParam(NAMES);
        String kindRegexMask = ctx.queryParam(LOCATION_KIND_LIKE);
        String office = ctx.queryParam(OFFICE);

        String formatParm = ctx.queryParamAsClass(Formats.JSONV2, String.class).getOrDefault("");
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm, Location.class);

        String results;

        List<CwmsIdLocationKind> locationKinds = locationsDao.getLocationKinds(names, kindRegexMask, office);
        results = Formats.format(contentType, locationKinds, CwmsIdLocationKind.class);
        ctx.result(results);
        requestResultSize.update(results.length());

        ctx.contentType(contentType.toString());
        ctx.status(HttpServletResponse.SC_OK);
    }
}
