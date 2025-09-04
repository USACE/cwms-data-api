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

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.STATUS_200;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.ApiServlet;
import cwms.cda.data.dto.CdaVersion;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;

public final class CdaVersionHandler implements Handler {
    static final String TAG = "Version";
    private final MetricRegistry metrics;

    public CdaVersionHandler(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        description = "Determine the current active version of CWMS Data Access.",
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(type = Formats.JSON, from = CdaVersion.class)})
        },
        tags = {TAG},
        path = "/version",
        method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            CdaVersion cdaVersion = new CdaVersion.Builder()
                .withVersion(ApiServlet.getApiVersion())
                .build();
            String serialized = Formats.format(new ContentType(Formats.JSON), cdaVersion);
            ctx.result(serialized);
            ctx.contentType(Formats.JSON);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }
}
