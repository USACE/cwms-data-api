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
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DATE;
import static cwms.cda.api.Controllers.DATUM;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.EFFECTIVE_DATE;
import static cwms.cda.api.Controllers.EFFECTIVE_DATE_EXACT;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.INCLUDE_ALIASES;
import static cwms.cda.api.Controllers.LEVEL_ID;
import static cwms.cda.api.Controllers.LEVEL_ID_MASK;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.addDeprecatedContentTypeWarning;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.api.Controllers.queryParamAsZdt;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevelRefs;
import cwms.cda.data.dto.locationlevel.LocationLevels;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import cwms.cda.data.dto.locationlevel.VirtualLocationLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.UnsupportedFormatException;
import cwms.cda.helpers.DateUtils;
import cwms.cda.helpers.annotations.IgnoreRequiredQueryParamMismatch;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import io.javalin.http.HttpResponseException;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class LevelRefsController implements Handler {
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    private static final int DEFAULT_PAGE_SIZE = 100;


    public LevelRefsController(MetricRegistry metrics) {
        this.metrics = metrics;

        requestResultSize = this.metrics.histogram((name(this.getClass().getName(), RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = LEVEL_ID_MASK, description = "Specifies the name(s) of "
                        + "the location level(s) whose data is to be included in the response. "
                        + "Uses * for all."),
                @OpenApiParam(name = OFFICE, description = "Specifies the owning "
                        + "office of the location level(s) whose data is to be included in the"
                        + " response. If this field is not specified, matching location level "
                        + "information from all offices shall be returned."),
                @OpenApiParam(name = BEGIN, description = "Specifies the start of the time "
                        + "window for data to be included in the response. If this field is "
                        + "not specified, no beginning time will be used."),
                @OpenApiParam(name = END, description = "Specifies the end of the time "
                        + "window for data to be included in the response. If this field is "
                        + "not specified, no end time will be used."),
                @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of "
                        + "the values of the begin and end fields (unless otherwise "
                        + "specified), as well as the time zone of any times in the response."
                        + " If this field is not specified, the default time zone of UTC "
                        + "shall be used."),
                @OpenApiParam(name = INCLUDE_ALIASES, description = "Whether to include the "
                        + "aliases for the location levels in the response. The default is false.",
                        type = Boolean.class),
                @OpenApiParam(name = PAGE, description = "This identifies where in the "
                        + "request you are. This is an opaque value, and can be obtained from "
                        + "the 'next-page' value in the response."),
                @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "How "
                        + "many entries per page returned. Default " + DEFAULT_PAGE_SIZE + "."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = ""),
                    @OpenApiContent(from = LocationLevels.class, type = Formats.JSONV1),
                    @OpenApiContent(from = LocationLevels.class, type = Formats.DEFAULT),
                    @OpenApiContent(from = LocationLevels.class, type = Formats.JSON),
                })
            },
            tags = LevelsController.TAG)
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(dsl);
            String levelIdMask = queryParamAsClass(ctx, new String[] {LEVEL_ID_MASK, NAME},
                String.class, null, metrics,
                name(LevelRefsController.class.getName(), GET_ALL));
            String office = ctx.queryParam(OFFICE);
            boolean includeAliases = ctx.queryParamAsClass(INCLUDE_ALIASES, Boolean.class)
                .getOrDefault(false);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, LocationLevelRefs.class);
            String cursor = ctx.queryParamAsClass(PAGE, String.class)
                .getOrDefault("");
            int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class)
                .getOrDefault(DEFAULT_PAGE_SIZE);
            Instant endZdt = queryParamAsInstant(ctx, END);
            Instant beginZdt = queryParamAsInstant(ctx, BEGIN);

            LocationLevelRefs levels = levelsDao.retrieveLocationLevelRefs(cursor, pageSize, levelIdMask,
                office, beginZdt, endZdt, includeAliases);
            String result = Formats.format(contentType, levels);
            ctx.result(result);
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
            ctx.contentType(contentType.toString());
        }
    }
}
