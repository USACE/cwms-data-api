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

package cwms.cda.api.texttimeseries;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.BaseHandler;
import cwms.cda.api.RangeRequestUtil;
import cwms.cda.data.dao.ClobDao;
import cwms.cda.data.dao.StreamConsumer;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jooq.DSLContext;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;


public class TextTimeSeriesValueController extends BaseHandler {
    public static final String TEXT_PLAIN = "text/plain";

    public TextTimeSeriesValueController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the id of the "
                            + "text timeseries"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Text TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = CLOB_ID, description = "Will be removed in a schema update. " +
                            "This is a placeholder for integration testing with schema 23.3.16", deprecated = true,
                            required = true)
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
        //https://www.hec.usace.army.mil/confluence/spaces/CWMS/pages/183110112/2024-02-29+Developer+Meeting+Task2A+Text-ts+and+Binary-ts+Design
        logUnusedPathParameter(ctx, NAME, "Handled as " + CLOB_ID + " in query parameter.  May change with schema.");
        String textId = requiredParam(ctx, CLOB_ID);
        String officeId = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            ClobDao clobDao = new ClobDao(dsl);

            StreamConsumer consumer = (is, isPosition, mediaType, totalLength) -> {
                updateResultSize(totalLength);
                ctx.header(Header.ACCEPT_RANGES, "bytes");
                RangeRequestUtil.seekableStream(ctx, is, isPosition, mediaType, totalLength);
            };
            clobDao.getClob(textId, officeId, consumer);

        }
    }
}
