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

package cwms.cda.data.dto.project;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@JsonDeserialize(builder = Lock.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
public class Lock extends CwmsDTO {
    // officeId held by CwmsDTO
    private final String projectId;
    private final String applicationId;
    private final String acquireTime;
    private final String sessionUser;
    private final String osUser;
    private final String sessionProgram;
    private final String sessionMachine;

    private Lock(Builder builer) {
        super(builer.officeId);
        this.projectId = builer.projectId;
        this.applicationId = builer.applicationId;
        this.acquireTime = builer.acquireTime;
        this.sessionUser = builer.sessionUser;
        this.osUser = builer.osUser;
        this.sessionProgram = builer.sessionProgram;
        this.sessionMachine = builer.sessionMachine;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getAcquireTime() {
        return acquireTime;
    }

    public String getSessionUser() {
        return sessionUser;
    }

    public String getOsUser() {
        return osUser;
    }

    public String getSessionProgram() {
        return sessionProgram;
    }

    public String getSessionMachine() {
        return sessionMachine;
    }

    @Override
    public void validate() throws FieldException {
        // Nothing to do
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String projectId;
        private String applicationId;
        private String acquireTime;
        private String sessionUser;
        private String osUser;
        private String sessionProgram;
        private String sessionMachine;

        public Builder() {
        }

        public Builder(String officeId, String projectId, String applicationId) {
            this.officeId = officeId;
            this.projectId = projectId;
            this.applicationId = applicationId;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withApplicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public Builder withAcquireTime(String acquireTime) {
            this.acquireTime = acquireTime;
            return this;
        }

        public Builder withSessionUser(String sessionUser) {
            this.sessionUser = sessionUser;
            return this;
        }

        public Builder withOsUser(String osUser) {
            this.osUser = osUser;
            return this;
        }

        public Builder withSessionProgram(String sessionProgram) {
            this.sessionProgram = sessionProgram;
            return this;
        }

        public Builder withSessionMachine(String sessionMachine) {
            this.sessionMachine = sessionMachine;
            return this;
        }

        public Builder from(Lock lock) {
            return this.withOfficeId(lock.officeId)
                    .withProjectId(lock.projectId)
                    .withApplicationId(lock.applicationId)
                    .withAcquireTime(lock.acquireTime)
                    .withSessionUser(lock.sessionUser)
                    .withOsUser(lock.osUser)
                    .withSessionProgram(lock.sessionProgram)
                    .withSessionMachine(lock.sessionMachine);
        }

        public Lock build() {
            return new Lock(this);
        }
    }
}
