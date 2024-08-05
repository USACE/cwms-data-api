/*
 *
 *  MIT License
 *
 *  Copyright (c) 2024 Hydrologic Engineering Center
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 *
 */

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonDeserialize(builder = WaterUser.Builder.class)
public final class WaterUser extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String entityName;
    @JsonProperty(required = true)
    private final CwmsId projectId;
    @JsonProperty(required = true)
    private final String waterRight;

    private WaterUser(Builder builder) {
        this.entityName = builder.entityName;
        this.projectId = builder.projectId;
        this.waterRight = builder.waterRight;
    }

    public CwmsId getProjectId() {
        return this.projectId;
    }

    public String getEntityName() {
        return this.entityName;
    }

    public String getWaterRight() {
        return this.waterRight;
    }

    public static class Builder {
        private String entityName;
        private CwmsId projectId;
        private String waterRight;

        public Builder withEntityName(@JsonProperty("entity-name") String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder withProjectId(@JsonProperty("project-id") CwmsId projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withWaterRight(@JsonProperty("water-right") String waterRight) {
            this.waterRight = waterRight;
            return this;
        }

        public WaterUser build() {
            return new WaterUser(this);
        }
    }
}
