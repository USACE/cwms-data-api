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
import cwms.cda.data.dao.ForecastInstanceDao;
import cwms.cda.helpers.DateUtils;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.time.Instant;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class ForecastFileController implements Handler {
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;


    public ForecastFileController(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((name(BinaryTimeSeriesValueController.class, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            description = "Used to download forecast file for the given parameters",
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "spec id of the forecast spec whose forecast instance data is to be "
                            + "included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = FORECAST_DATE, required = true, description = "Specifies the "
                            + "forecast date time of the forecast instance to be retrieved."),
                    @OpenApiParam(name = ISSUE_DATE, required = true, description = "Specifies the "
                            + "issue date time of the forecast instance to be retrieved."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the forecast spec whose forecast instance is to be "
                            + "included in the response."),
                    @OpenApiParam(name = DESIGNATOR, required = true, description = "Specifies the "
                            + "designator of the forecast spec whose forecast instance data to be included "
                            + "in the response."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = byte[].class)
                            }
                    ),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a forecast instance."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            tags = {ForecastSpecController.TAG}
    )
    public void handle(Context ctx) {
        String specId = requiredParam(ctx, NAME);
        String office = requiredParam(ctx, OFFICE);
        String designator = requiredParam(ctx, DESIGNATOR);
        String forecastDate =  requiredParam(ctx, FORECAST_DATE);
        String issueDate = requiredParam(ctx, ISSUE_DATE);
        Instant forecastInstant = DateUtils.parseUserDate(forecastDate, "UTC").toInstant();
        Instant issueInstant = DateUtils.parseUserDate(issueDate, "UTC").toInstant();
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            ForecastInstanceDao dao = new ForecastInstanceDao(getDslContext(ctx));
            dao.getFileBlob(office, specId, designator, forecastInstant, issueInstant, (blob, mediaType) -> {
                if (blob == null) {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                            + "blob based on given parameters"));
                } else {
                    long size = blob.length();
                    requestResultSize.update(size);
                    InputStream is = blob.getBinaryStream();
                    ctx.seekableStream(is, mediaType, size);
                }
            });
        }
    }
}