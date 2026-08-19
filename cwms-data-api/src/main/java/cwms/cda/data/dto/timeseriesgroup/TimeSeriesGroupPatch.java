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

package cwms.cda.data.dto.timeseriesgroup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A PATCH of a timeseries group, describing time series "
        + "including membership describing assignment and unassignment of time series to the group.")
@JsonRootName("timeseries-group")
@JsonDeserialize(builder = TimeSeriesGroupPatch.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV2.class)
public final class TimeSeriesGroupPatch extends CwmsDTO {
    @JsonProperty(required = true)
    private final String id;

    @JsonProperty(required = true)
    private final TimeSeriesCategory timeSeriesCategory;

    private final String description;
    private final String sharedAliasId;
    private final String sharedRefTsId;
    private final Membership membership;

    private TimeSeriesGroupPatch(Builder builder) {
        super(builder.officeId);
        this.id = builder.id;
        this.timeSeriesCategory = builder.timeSeriesCategory != null
                ? new TimeSeriesCategory(builder.timeSeriesCategory) : null;
        this.description = builder.description;
        this.sharedAliasId = builder.sharedAliasId;
        this.sharedRefTsId = builder.sharedRefTsId;
        this.membership = builder.membership;
    }

    public String getId() {
        return id;
    }

    public TimeSeriesCategory getTimeSeriesCategory() {
        return timeSeriesCategory;
    }

    public String getDescription() {
        return description;
    }

    public String getSharedAliasId() {
        return sharedAliasId;
    }

    public String getSharedRefTsId() {
        return sharedRefTsId;
    }

    public Membership getMembership() {
        return membership;
    }

    public static class Builder {
        private String officeId;
        private String id;
        private TimeSeriesCategory timeSeriesCategory;
        private String description;
        private String sharedAliasId;
        private String sharedRefTsId;
        private Membership membership;

        public Builder() {
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withTimeSeriesCategory(TimeSeriesCategory timeSeriesCategory) {
            this.timeSeriesCategory = timeSeriesCategory;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withSharedAliasId(String sharedAliasId) {
            this.sharedAliasId = sharedAliasId;
            return this;
        }

        public Builder withSharedRefTsId(String sharedRefTsId) {
            this.sharedRefTsId = sharedRefTsId;
            return this;
        }

        public Builder withMembership(Membership membership) {
            this.membership = membership;
            return this;
        }

        @JsonIgnore
        public Builder from(TimeSeriesGroupPatch patch) {
            if (patch != null) {
                this.officeId = patch.getOfficeId();
                this.id = patch.getId();
                this.timeSeriesCategory = patch.getTimeSeriesCategory();
                this.description = patch.getDescription();
                this.sharedAliasId = patch.getSharedAliasId();
                this.sharedRefTsId = patch.getSharedRefTsId();
                this.membership = patch.getMembership();
            }
            return this;
        }

        public TimeSeriesGroupPatch build() {
            return new TimeSeriesGroupPatch(this);
        }
    }
}
