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
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;

//name in json is just "membership"
@Schema(description = "Describes time series to assign to, and/or unassign from, a timeseries group")
@JsonDeserialize(builder = TimeSeriesGroupMembership.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonRootName("membership")
public final class TimeSeriesGroupMembership extends CwmsDTOBase {
    private final List<AssignedTimeSeries> assign;
    private final List<CwmsId> unassign;

    private TimeSeriesGroupMembership(Builder builder) {
        this.assign = builder.assign != null ? builder.assign : new ArrayList<>();
        this.unassign = builder.unassign != null ? builder.unassign : new ArrayList<>();
    }

    @Schema(description = "Time series to assign to the group")
    public List<AssignedTimeSeries> getAssign() {
        return assign;
    }

    @Schema(description = "Time series to unassign from the group")
    public List<CwmsId> getUnassign() {
        return unassign;
    }

    public static class Builder {
        private List<AssignedTimeSeries> assign;
        private List<CwmsId> unassign;

        public Builder() {
        }

        public Builder withAssign(List<AssignedTimeSeries> assign) {
            this.assign = assign != null ? new ArrayList<>(assign) : null;
            return this;
        }

        public Builder withUnassign(List<CwmsId> unassign) {
            this.unassign = unassign != null ? new ArrayList<>(unassign) : null;
            return this;
        }

        @JsonIgnore
        public Builder from(TimeSeriesGroupMembership membership) {
            if (membership != null) {
                this.assign = membership.getAssign();
                this.unassign = membership.getUnassign();
            }
            return this;
        }

        public TimeSeriesGroupMembership build() {
            return new TimeSeriesGroupMembership(this);
        }
    }
}
