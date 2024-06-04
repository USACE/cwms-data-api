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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.CountyDao;
import cwms.cda.data.dto.County;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.ContentTypeAliasMap;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

/**
 * Handles all county CRUD methods.
 *
 * @see CountyController
 */
public class CountyController implements CrudHandler {
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;
    private static final ContentTypeAliasMap CONTENT_TYPE_ALIAS_MAP = new ContentTypeAliasMap();

    static
    {
        CONTENT_TYPE_ALIAS_MAP.addContentType(Formats.JSON, new ContentType(Formats.JSONV2));
        CONTENT_TYPE_ALIAS_MAP.addContentType(Formats.DEFAULT, new ContentType(Formats.JSONV2));
    }

    /**
     * Sets up county endpoint metrics for the controller.
     *
     * @param metrics set the MetricRegistry for this class
     */
    public CountyController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = CountyController.class.getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            responses = {
                @OpenApiResponse(status = "" + HttpServletResponse.SC_OK,
                        description = "A list of counties.",
                        content = {
                            @OpenApiContent(from = County.class, isArray = true,
                                    type =  Formats.JSONV2),
                        }),
            },
            tags = {"Counties"}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            CountyDao dao = new CountyDao(dsl);
            List<County> counties = dao.getCounties();
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, CONTENT_TYPE_ALIAS_MAP);
            if (contentType == null) {
                throw new FormattingException("Format header could not be parsed");
            }
            String result = Formats.format(contentType, counties, County.class);
            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, @NotNull String county) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, @NotNull String county) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, @NotNull String county) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
