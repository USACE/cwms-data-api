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
import cwms.cda.data.dao.ClobDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;


public class TextTimeSeriesValueController implements Handler {
    public static final String TEXT_PLAIN = "text/plain";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public TextTimeSeriesValueController(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((name(TextTimeSeriesValueController.class, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the id of the "
                            + "text timeseries"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Text TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = DATE, required = true, description = "The date of the text value to retrieve"),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the value to retrieve."),
                    @OpenApiParam(name = CLOB_ID, description = "Will be removed in a schema update. " +
                            "This is a placeholder for integration testing with schema 23.3.16", deprecated = true)
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = String.class)
                            }
                    )},
            tags = {TextTimeSeriesController.TAG}
    )
    public void handle(Context ctx) {
        //Implementation will change with new CWMS schema
        //https://www.hec.usace.army.mil/confluence/display/CWMS/2024-02-29+Task2A+Text-ts+and+Binary-ts+Design
        String textId = requiredParam(ctx, CLOB_ID);
        String officeId = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            ClobDao clobDao = new ClobDao(dsl);
            clobDao.getClob(textId, officeId, clob -> {
                if (clob == null) {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                            + "clob based on given parameters"));
                } else {
                    long size = clob.length();
                    requestResultSize.update(size);
                    InputStream is = clob.getAsciiStream();
                    ctx.seekableStream(is, TEXT_PLAIN, size);
                }
            });
        }
    }
}
