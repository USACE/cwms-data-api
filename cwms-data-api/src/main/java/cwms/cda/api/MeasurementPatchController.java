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
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import cwms.cda.data.dao.MeasurementDao;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class MeasurementPatchController implements Handler {

    private final MetricRegistry metrics;

    public MeasurementPatchController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(isArray = true, from = Measurement.class, type = Formats.JSONV1),
                            @OpenApiContent(from = Measurement.class, type = Formats.JSON)
                    },
                    required = true),
            description = "Update Measurement",
            method = HttpMethod.PATCH,
            tags = {MeasurementController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Measurement(s) successfully updated.")
            }
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context ignored = markAndTime()) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Measurement.class);
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            List<Measurement> measurements = MeasurementController.parseMeasurements(ctx, contentType);
            if(measurements.size() == 1) {
                dao.updateMeasurement(measurements.get(0));
                ctx.status(HttpServletResponse.SC_OK).json("Updated Measurement");
            } else {
                dao.updateMeasurements(measurements);
                ctx.status(HttpServletResponse.SC_OK).json("Updated Measurements");
            }
        }
    }

    private Timer.Context markAndTime() {
        return Controllers.markAndTime(metrics, getClass().getName(), Controllers.UPDATE);
    }
}
