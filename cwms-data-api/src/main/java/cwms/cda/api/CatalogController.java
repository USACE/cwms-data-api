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

package cwms.cda.api;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.semconv.SemanticAttributes;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

import java.util.logging.Logger;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;

public class CatalogController implements CrudHandler {

    private static final Logger logger = Logger.getLogger(CatalogController.class.getName());
    private static final String TAG = "Catalog-Beta";


    private final OpenTelemetry otelSdk;

    private final LongHistogram requestResultSize;

    private final int defaultPageSize = 500;
    private final Tracer tracer;

    public CatalogController(OpenTelemetry otelSdk) {
        this.otelSdk = otelSdk;
        requestResultSize = otelSdk.getMeter(getClass().getName())
                .histogramBuilder("results_size")
                .ofLongs()
                .setDescription("Size of the results returned from the catalog")
                .build();
        tracer = otelSdk.getTracer(getClass().getName());
    }

    @OpenApi(tags = {TAG}, ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void delete(Context ctx, @NotNull String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void getAll(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).result("cannot perform this action");
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are."
                    ),
                    @OpenApiParam(name = CURSOR,
                            deprecated = true,
                            description = "Deprecated. Use 'page' instead."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entires per page returned. Default 500."
                    ),
                    @OpenApiParam(name = PAGESIZE3,
                            deprecated = true,
                            type = Integer.class,
                            description = "Deprecated. Use page-size."
                    ),
                    @OpenApiParam(name = UNITSYSTEM2,
                            deprecated = true,
                            type = UnitSystem.class,
                            description = "Deprecated. Use unit-system."
                    ),
                    @OpenApiParam(name = UNIT_SYSTEM,
                            type = UnitSystem.class,
                            description = UnitSystem.DESCRIPTION
                    ),
                    @OpenApiParam(name = OFFICE,
                            description = "3-4 letter office name representing the district you "
                                    + "want to isolate data to."
                    ),
                    @OpenApiParam(name = LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the id"
                    ),
                    @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the "
                                    + "timeseries category id"
                    ),
                    @OpenApiParam(name = TIMESERIESCATEGORYLIKE2,
                            deprecated = true,
                            description = "Deprecated. Use timeseries-category-like."
                    ),
                    @OpenApiParam(name = TIMESERIES_GROUP_LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the "
                                    + "timeseries group id"
                    ),
                    @OpenApiParam(name = TIMESERIES_GROUP_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use timeseries-group-like."
                    ),
                    @OpenApiParam(name = LOCATION_CATEGORY_LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the location"
                                    + " category id"
                    ),
                    @OpenApiParam(name = LOCATION_CATEGORY_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use location-category-like."
                    ),
                    @OpenApiParam(name = LOCATION_GROUP_LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> matching against the location"
                                    + " group id"
                    ),
                    @OpenApiParam(name = LOCATION_GROUP_LIKE2,
                            deprecated = true,
                            description = "Deprecated. Use location-group-like."
                    ),
                    @OpenApiParam(name = BOUNDING_OFFICE_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                            + "matching against the location bounding office. "
                            + "When this field is used items with no bounding office set will not be present in results."),
            },
            pathParams = {
                    @OpenApiParam(name = "dataset",
                            type = CatalogableEndpoint.class,
                            description = "A list of what data? E.g. Timeseries, Locations, "
                                    + "Ratings, etc")
            },
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "A list of elements the data set you've selected.",
                    content = {
                            @OpenApiContent(from = Catalog.class, type = Formats.JSONV2),
                            @OpenApiContent(from = Catalog.class, type = Formats.XML)
                    }
            )
            },
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, @NotNull String dataSet) {
        Span span = tracer.spanBuilder("getOne")
                .setAttribute(SemanticAttributes.HTTP_REQUEST_METHOD, "GET")
                .setAttribute(SemanticAttributes.URL_FULL, ctx.fullUrl())
                .setAttribute(SemanticAttributes.URL_PATH, ctx.path())
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        try (Scope scope = span.makeCurrent(); DSLContext dsl = JooqDao.getDslContext(ctx)) {
            String valDataSet =
                    ((PolicyFactory) ctx.appAttribute("PolicyFactory")).sanitize(dataSet);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", otelSdk, name(CatalogController.class.getName(), GET_ONE));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE, PAGESIZE3,
                    PAGESIZE2}, Integer.class, defaultPageSize, otelSdk,
                    name(CatalogController.class.getName(), GET_ONE));

            String unitSystem = queryParamAsClass(ctx,
                    new String[]{UNIT_SYSTEM, UNITSYSTEM2},
                    String.class, UnitSystem.SI.getValue(), otelSdk,
                    name(CatalogController.class.getName(), GET_ONE));

            String office = ctx.queryParamAsClass(OFFICE, String.class).allowNullable()
                            .check(Office::validOfficeCanNull, "Invalid office provided")
                            .get();

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String tsCategoryLike = queryParamAsClass(ctx, new String[]{TIMESERIES_CATEGORY_LIKE, TIMESERIESCATEGORYLIKE2},
                    String.class, null, otelSdk, name(CatalogController.class.getName(), GET_ONE));

            String tsGroupLike = queryParamAsClass(ctx, new String[]{TIMESERIES_GROUP_LIKE, TIMESERIES_GROUP_LIKE2},
                    String.class, null, otelSdk, name(CatalogController.class.getName(), GET_ONE));

            String locCategoryLike = queryParamAsClass(ctx, new String[]{LOCATION_CATEGORY_LIKE, LOCATION_CATEGORY_LIKE2},
                    String.class, null, otelSdk, name(CatalogController.class.getName(), GET_ONE));

            String locGroupLike = queryParamAsClass(ctx, new String[]{LOCATION_GROUP_LIKE, LOCATION_GROUP_LIKE2},
                    String.class, null, otelSdk, name(CatalogController.class.getName(), GET_ONE));

            String boundingOfficeLike = queryParamAsClass(ctx, new String[]{BOUNDING_OFFICE_LIKE},
                    String.class, null, otelSdk, name(CatalogController.class.getName(), GET_ONE));

            String acceptHeader = ctx.header(ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, null);
            Catalog cat = null;
            if (TIMESERIES.equalsIgnoreCase(valDataSet)) {
                TimeSeriesDao tsDao = new TimeSeriesDaoImpl(dsl, tracer);
                cat = tsDao.getTimeSeriesCatalog(cursor, pageSize, office, like, locCategoryLike,
                        locGroupLike, tsCategoryLike, tsGroupLike, boundingOfficeLike);
            } else if (LOCATIONS.equalsIgnoreCase(valDataSet)) {
                LocationsDao dao = new LocationsDaoImpl(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, unitSystem, office, like,
                        locCategoryLike, locGroupLike, boundingOfficeLike);
            }
            if (cat != null) {
                String data = Formats.format(contentType, cat);
                ctx.result(data).contentType(contentType.toString());
                requestResultSize.record(data.length());
            } else {
                final CdaError re = new CdaError("Cannot create catalog of requested "
                        + "information");

                logger.info(() -> re + "with url:" + ctx.fullUrl());
                ctx.json(re).status(HttpCode.NOT_FOUND);
            }
        } finally {
            span.end();
        }
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void update(Context ctx, @NotNull String entry) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
