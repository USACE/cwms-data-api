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

package cwms.cda.data.dao.project;

import cwms.cda.data.dto.project.ProjectLockV1;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.exception.TooManyRowsException;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;

public final class ProjectLockDaoV1 extends ProjectLockDao<ProjectLockV1> {

    public ProjectLockDaoV1(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public String requestLock(ProjectLockV1 request, boolean revokeExisting, int revokeTimeout) {

        String office = request.getOfficeId();
        return connectionResult(dsl, c -> {
            Configuration configuration = getDslContext(c, office).configuration();
            return CWMS_PROJECT_PACKAGE.call_REQUEST_LOCK(configuration, request.getProjectId(),
                request.getApplicationId(), formatBool(revokeExisting), BigInteger.valueOf(revokeTimeout),
                office, request.getSessionUser(), request.getOsUser(), request.getSessionProgram(),
                request.getSessionMachine());
        });
    }

    @Override
    public List<ProjectLockV1> retrieveLocks(String officeMask, String projMask, String appMask) {
        return CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projMask, appMask, "UTC", officeMask)
                .map(ProjectLockDaoV1::buildLockFromCatLocksRecord);
    }

    @Override
    public ProjectLockV1 retrieveLock(String office, String projectName, String applicationName) {
        ProjectLockV1 retval = null;

        List<ProjectLockV1> locks = CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projectName, applicationName, "UTC", office)
                .map(ProjectLockDaoV1::buildLockFromCatLocksRecord);
        if (locks.size() > 1) {
            throw new TooManyRowsException("Provided arguments matched " + locks.size() + " rows");
        } else if (locks.size() == 1) {
            retval = locks.get(0);
        }

        return retval;
    }

    private static @NotNull ProjectLockV1 buildLockFromCatLocksRecord(Record catRecord) {
        String officeId = catRecord.getValue(OFFICE_ID, String.class);
        String projectId = catRecord.getValue(PROJECT_ID, String.class);
        String applicationId = catRecord.getValue(APPLICATION_ID, String.class);

        String acquireStr = catRecord.getValue(ACQUIRE_TIME, String.class);
        Instant acquireTime = acquireStr != null ? Instant.parse(acquireStr) : null;

        return new ProjectLockV1.Builder(officeId, projectId, applicationId)
                .withAcquireTime(acquireTime)
                .withSessionUser(catRecord.getValue(SESSION_USER, String.class))
                .withOsUser(catRecord.getValue(OS_USER, String.class))
                .withSessionProgram(catRecord.getValue(SESSION_PROGRAM, String.class))
                .withSessionMachine(catRecord.getValue(SESSION_MACHINE, String.class))
                .build()
                ;
    }
}
