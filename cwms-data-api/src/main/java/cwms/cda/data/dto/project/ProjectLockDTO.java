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

package cwms.cda.data.dto.project;

import cwms.cda.data.dto.CwmsDTOBase;
import java.time.Instant;

abstract class ProjectLockDTO extends CwmsDTOBase implements ProjectLock {
    private final String applicationId;
    private final Instant acquireTime;
    private final String sessionUser;
    private final String osUser;
    private final String sessionProgram;
    private final String sessionMachine;

    ProjectLockDTO(String applicationId, Instant acquireTime, String sessionUser, String osUser,
                             String sessionProgram, String sessionMachine) {
        this.applicationId = applicationId;
        this.acquireTime = acquireTime;
        this.sessionUser = sessionUser;
        this.osUser = osUser;
        this.sessionProgram = sessionProgram;
        this.sessionMachine = sessionMachine;
    }

    @Override
    public String getApplicationId() {
        return applicationId;
    }

    @Override
    public Instant getAcquireTime() {
        return acquireTime;
    }

    @Override
    public String getSessionUser() {
        return sessionUser;
    }

    @Override
    public String getOsUser() {
        return osUser;
    }

    @Override
    public String getSessionProgram() {
        return sessionProgram;
    }

    @Override
    public String getSessionMachine() {
        return sessionMachine;
    }

    abstract static class Builder<B extends Builder<B>> {
        String applicationId;
        Instant acquireTime;
        String sessionUser;
        String osUser;
        String sessionProgram;
        String sessionMachine;

        abstract B self();

        public B withApplicationId(String applicationId) {
            this.applicationId = applicationId;
            return self();
        }

        public B withAcquireTime(Instant acquireTime) {
            this.acquireTime = acquireTime;
            return self();
        }

        public B withSessionUser(String sessionUser) {
            this.sessionUser = sessionUser;
            return self();
        }

        public B withOsUser(String osUser) {
            this.osUser = osUser;
            return self();
        }

        public B withSessionProgram(String sessionProgram) {
            this.sessionProgram = sessionProgram;
            return self();
        }

        public B withSessionMachine(String sessionMachine) {
            this.sessionMachine = sessionMachine;
            return self();
        }

        public B from(ProjectLockDTO lock) {
            return withApplicationId(lock.getApplicationId())
                    .withAcquireTime(lock.getAcquireTime())
                    .withSessionUser(lock.getSessionUser())
                    .withOsUser(lock.getOsUser())
                    .withSessionProgram(lock.getSessionProgram())
                    .withSessionMachine(lock.getSessionMachine());
        }
    }
}
