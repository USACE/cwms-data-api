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

package cwms.cda.api.project;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;


public abstract class ProjectLockGetOne<T extends CwmsDTOBase & ProjectLock> implements Handler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    public static final String TAGS = "Project Locks";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    protected Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected ProjectLockGetOne(MetricRegistry metrics, Class<?> metricsIdentity) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((
                name(metricsIdentity, Controllers.RESULTS, Controllers.SIZE)));
    }

    protected abstract ProjectLockDao<T> getDao(Context ctx);

    protected abstract Class<T> lockClass();

    protected abstract String getOffice(Context ctx);

    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String office = getOffice(ctx);
        String prjId = ctx.pathParam(NAME);
        String appId = requiredParam(ctx, APPLICATION_ID);

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            T lock = getDao(ctx).retrieveLock(office, prjId, appId);
            if (lock != null) {
                String acceptHeader = ctx.header(Header.ACCEPT);
                ContentType acceptType = Formats.parseHeader(acceptHeader, lockClass());
                String result = Formats.format(acceptType, lock);
                ctx.contentType(acceptType.toString());
                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);

                byte[] bytes = result.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            } else {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Project Lock", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Project Lock");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

}
