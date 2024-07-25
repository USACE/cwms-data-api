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

@JsonDeserialize(builder = LockRevokerRights.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
public class LockRevokerRights extends CwmsDTO {

    private final String projectId;
    private final String applicationId;
    private final String userId;

    private LockRevokerRights(Builder builder) {
        super(builder.officeId);
        this.projectId = builder.projectId;
        this.applicationId = builder.applicationId;
        this.userId = builder.userId;
    }


    public String getProjectId() {
        return projectId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getUserId() {
        return userId;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String officeId;
        private String projectId;
        private String applicationId;
        private String userId;

        public Builder() {
        }

        public Builder(String officeId, String projectId, String applicationId, String userId) {
            this.officeId = officeId;
            this.projectId = projectId;
            this.applicationId = applicationId;
            this.userId = userId;
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

        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder from(LockRevokerRights lockRevokerRights) {
            return withOfficeId(lockRevokerRights.getOfficeId())
                    .withApplicationId(lockRevokerRights.getApplicationId())
                    .withProjectId(lockRevokerRights.getProjectId())
                    .withUserId(lockRevokerRights.getUserId());
        }

        public LockRevokerRights build() {
            return new LockRevokerRights(this);
        }
    }
}
