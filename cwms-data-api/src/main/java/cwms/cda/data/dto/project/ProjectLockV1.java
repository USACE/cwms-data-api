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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonRootName("project-lock")
@JsonDeserialize(builder = ProjectLockV1.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public final class ProjectLockV1 extends ProjectLockDTO {
    @Schema(description = "Owning office of object.")
    @JsonProperty(required = true)
    private final String officeId;
    private final String projectId;

    private ProjectLockV1(Builder builder) {
        super(builder.applicationId, builder.acquireTime, builder.sessionUser, builder.osUser,
                builder.sessionProgram, builder.sessionMachine);
        this.officeId = builder.officeId;
        this.projectId = builder.projectId;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getProjectId() {
        return projectId;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends ProjectLockDTO.Builder<Builder> {
        private String officeId;
        private String projectId;

        public Builder() {
        }

        public Builder(ProjectLockV1 lock) {
            this.officeId = lock.officeId;
            this.projectId = lock.projectId;
            from((ProjectLockDTO) lock);
        }

        public Builder(String officeId, String projectId, String applicationId) {
            this.officeId = officeId;
            this.projectId = projectId;
            this.applicationId = applicationId;
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder from(ProjectLockV1 lock) {
            this.officeId = lock.officeId;
            this.projectId = lock.projectId;
            return from((ProjectLockDTO) lock);
        }

        public ProjectLockV1 build() {
            return new ProjectLockV1(this);
        }
    }
}
