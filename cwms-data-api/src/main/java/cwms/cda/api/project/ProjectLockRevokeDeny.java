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

import static cwms.cda.api.Controllers.LOCK_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.ProjectLock;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;


public abstract class ProjectLockRevokeDeny<T extends ProjectLock> implements Handler {
    public static final String TAGS = "Project Locks";
    private final MetricRegistry metrics;

    protected Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected ProjectLockRevokeDeny(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    protected abstract ProjectLockDao<T> getDao(Context ctx);

    protected abstract String getOffice(Context ctx);

    @Override
    public void handle(@NotNull Context ctx) throws Exception {

        String lockId = requiredParam(ctx, LOCK_ID);
        String office = getOffice(ctx);

        try (final Timer.Context ignored = markAndTime("deny")) {
            getDao(ctx).denyLockRevocation(office, lockId);
        }
        ctx.status(HttpServletResponse.SC_OK);
    }

}
