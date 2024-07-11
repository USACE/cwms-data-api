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

package cwms.cda.api.project;

import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.SOURCE_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMESERIES_ID;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.ProjectController;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.project.PublishStatusUpdateResult;
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
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;


public class ProjectPublishStatusUpdate implements Handler {
    public static final String TAG = ProjectController.TAG;

    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public ProjectPublishStatusUpdate(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
            description = "Publishes a message on the office's STATUS queue that a project has "
                    + "been updated for a specified application",
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "The office "
                        + "generating the message (and owning the project)."),
                @OpenApiParam(name = APPLICATION_ID, required = true, description = "A text "
                        + "string identifying the application for which the update applies."),
                @OpenApiParam(name = SOURCE_ID, description = "An application-defined string "
                        + "of the instance and/or component that generated the message. "
                        + "If NULL or not specified, the generated message will not include "
                        + "this item."),
                @OpenApiParam(name = TIMESERIES_ID, description = "A time series identifier of "
                        + "the time series associated with the update. If NULL or not "
                        + "specified, the generated message will not include this item."),
                @OpenApiParam(name = BEGIN, description = "The start time of the updates to "
                        + "the time series. If NULL or not specified, the generated message "
                        + "will not include this item."),
                @OpenApiParam(name = END, description = "The end time of the updates to the "
                        + "time series. If NULL or not specified, the generated message will "
                        + "not include this item.")
            },
            pathParams = {
                    @OpenApiParam(name = NAME,  description = "The location "
                            + "identifier of the project that has been updated"),
            },
            method = HttpMethod.POST,
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = PublishStatusUpdateResult.class)}
                    )},
            tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String projectId = ctx.pathParam(NAME);

        try (final Timer.Context ignored = markAndTime("publish")) {
            ProjectDao prjDao = new ProjectDao(JooqDao.getDslContext(ctx));
            String office = requiredParam(ctx, OFFICE);
            String appId = requiredParam(ctx, APPLICATION_ID);

            String sourceId = ctx.queryParam(SOURCE_ID);
            String tsId = ctx.queryParam(TIMESERIES_ID);
            Instant begin = queryParamAsInstant(ctx, BEGIN);
            Instant end = queryParamAsInstant(ctx, END);

            Instant timeOfMessage = prjDao.publishStatusUpdate(
                    office, projectId, appId, sourceId, tsId, begin, end);
            PublishStatusUpdateResult updateResult = new PublishStatusUpdateResult(timeOfMessage);

            ContentType contentType = Formats.parseHeader(ctx.header(Header.ACCEPT), PublishStatusUpdateResult.class);
            String result = Formats.format(contentType, updateResult);
            ctx.result(result).contentType(contentType.toString());
            ctx.status(HttpServletResponse.SC_OK);
        }

    }

}
