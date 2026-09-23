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
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@JsonRootName("project-lock")
@JsonDeserialize(builder = ProjectLockV2.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class, aliases = {Formats.DEFAULT})
public final class ProjectLockV2 extends ProjectLockDTO {
    @JsonProperty(required = true)
    private final CwmsId id;

    private ProjectLockV2(Builder builder) {
        super(builder.applicationId, builder.acquireTime, builder.sessionUser, builder.osUser,
                builder.sessionProgram, builder.sessionMachine);
        this.id = builder.id;
    }

    public CwmsId getId() {
        return id;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends ProjectLockDTO.Builder<Builder> {
        private CwmsId id;

        public Builder() {
        }

        public Builder(ProjectLockV2 lock) {
            this.id = lock.getId();
            from((ProjectLockDTO) lock);
        }

        public Builder(CwmsId id, String applicationId) {
            this.id = id;
            this.applicationId = applicationId;
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withId(CwmsId id) {
            this.id = id;
            return this;
        }

        public Builder from(ProjectLockV2 lock) {
            this.id = lock.getId();
            return from((ProjectLockDTO) lock);
        }

        public ProjectLockV2 build() {
            return new ProjectLockV2(this);
        }
    }
}
