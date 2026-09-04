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

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import java.time.Instant;

import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class TextTimeSeriesControllerV2 extends TextTimeSeriesController {

    public TextTimeSeriesControllerV2(MetricRegistry metrics) {
        super(metrics);
    }

    @Override
    protected String getOffice(@NotNull Context ctx) {
        return ctx.pathParam(OFFICE);
    }

    @OpenApi(
            summary = "Retrieve text time series values for a provided time window and date version."
                    + "If individual values exceed 64 kilobytes, a URL to a separate download is provided "
                    + "instead of being included in the returned payload from this request.",
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Text TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the ts-id of the "
                            + "text timeseries"),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = VERSION_DATE, description = "Specifies the version date of the "
                            + "text timeseries. If not specified, the latest version will be used."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSON, from = TextTimeSeries.class)
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        super.getAll(ctx);
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String templateId) {
        super.getOne(ctx, templateId);
    }

    @OpenApi(
            description = "Create new TextTimeSeries",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = TextTimeSeries.class, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = REPLACE_ALL, type = Boolean.class, description = "Whether to "
                            + "replace any and all existing text with the specified text. "
                            + "Default is " + DEFAULT_CREATE_REPLACE_ALL)},
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        super.create(ctx);
    }

    @OpenApi(
        description = "Updates a text timeseries. The request body may be a full or a partial "
                + "TextTimeSeries representation: per ADR-0011, the current resource (identified "
                + "by the path and the " + BEGIN + "/" + END + " window) is retrieved, the request "
                + "body is merged onto it, and the result is stored -- so any field omitted from "
                + "the body is left unchanged, and a field explicitly set to null clears that "
                + "field. Each entry in regular-text-values must include date-time -- that's what "
                + "identifies which entry is being patched; every other row field may be omitted.",
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning "
                    + "office of the text timeseries to be updated."),
            @OpenApiParam(name = NAME, description = "The id of the text timeseries to be updated"),
        },
        queryParams = {
            @OpenApiParam(name = BEGIN, required = true, description = "The start of the time "
                    + "window containing the date-times named in the request body."),
            @OpenApiParam(name = END, required = true, description = "The end of the time window "
                    + "containing the date-times named in the request body."),
            @OpenApiParam(name = VERSION_DATE, description = "Specifies the version date of the "
                    + "text timeseries. If not specified, the latest version will be used."),
            @OpenApiParam(name = REPLACE_ALL, type = Boolean.class, description = "Whether to "
                    + "replace any and all existing text with the specified text. "
                    + "Default is:" + DEFAULT_UPDATE_REPLACE_ALL)
        },
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TextTimeSeries.class, type = Formats.JSON),
            },
            required = true
        ),
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String tsId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class)
                .getOrDefault(DEFAULT_UPDATE_REPLACE_ALL);
            String office = getOffice(ctx);
            Instant begin = requiredInstant(ctx, BEGIN);
            Instant end = requiredInstant(ctx, END);
            Instant version = queryParamAsInstant(ctx, VERSION_DATE);

            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, TextTimeSeries.class);
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesTextDao dao = getDao(dsl);

            TextTimeSeries existing = dao.retrieveFromDao(office, tsId, "*", begin, end, version,
                    Integer.MAX_VALUE, null);
            TextTimeSeries updated = Formats.parsePatchContent(contentType, existing, ctx.bodyAsInputStream(), TextTimeSeries.class);

            dao.store(updated, replaceAll);
        }
    }

    @OpenApi(
            description = "Deletes requested text timeseries id",
            pathParams = {
                    @OpenApiParam(name = NAME, description = "The time series identifier to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries identifier to be deleted"),
                    @OpenApiParam(name = Controllers.TEXT_MASK, required = true, description = "The "
                            + "standard text pattern to match. "
                            + "Use glob-style wildcard characters instead of sql-style wildcard "
                            + "characters for pattern matching."
                            + "  For StandardTextTimeSeries this should be the Standard_Text_Id (such"
                            + " as 'E' for ESTIMATED)"),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time"
                            + " window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window."),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the time "
                            + "series.  If not specified, maximum version date is used.")
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String textTimeSeriesId) {
        super.delete(ctx, textTimeSeriesId);
    }
}
