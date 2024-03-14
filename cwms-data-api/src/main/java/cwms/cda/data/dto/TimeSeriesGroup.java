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

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import cwms.cda.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@Schema(description = "A representation of a timeseries group")
@XmlRootElement(name = "timeseries-group")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeSeriesGroup extends CwmsDTO {
    private String id;
    private TimeSeriesCategory timeSeriesCategory;
    private String description;

    private String sharedAliasId;
    private String sharedRefTsId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<AssignedTimeSeries> assignedTimeSeries = new ArrayList<>();


    public TimeSeriesGroup() {
        super(null);
    }

    public TimeSeriesGroup(TimeSeriesCategory cat, String grpOfficeId, String grpId, String grpDesc,
                           String sharedTsAliasId, String sharedRefTsId) {
        super(grpOfficeId);
        this.timeSeriesCategory = new TimeSeriesCategory(cat);
        this.id = grpId;
        this.description = grpDesc;
        this.sharedAliasId = sharedTsAliasId;
        this.sharedRefTsId = sharedRefTsId;

    }

    public TimeSeriesGroup(TimeSeriesGroup group) {
        super(group.getOfficeId());
        this.timeSeriesCategory = new TimeSeriesCategory(group.timeSeriesCategory);
        this.id = group.id;
        this.description = group.description;
        this.sharedAliasId = group.sharedAliasId;
        this.sharedRefTsId = group.sharedRefTsId;
        if (group.assignedTimeSeries != null) {
            if (group.assignedTimeSeries.isEmpty()) {
                this.assignedTimeSeries = new ArrayList<>();
            } else {
                this.assignedTimeSeries = new ArrayList<>(group.assignedTimeSeries);
            }
        } else {
            this.assignedTimeSeries = null;
        }

    }

    public TimeSeriesGroup(TimeSeriesGroup group, List<AssignedTimeSeries> timeSeries) {
        this(group);
        if (timeSeries != null && !timeSeries.isEmpty()) {
            this.assignedTimeSeries = new ArrayList<>(timeSeries);
        }
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

    public List<AssignedTimeSeries> getAssignedTimeSeries() {
        return assignedTimeSeries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TimeSeriesGroup that = (TimeSeriesGroup) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
            return false;
        }
        if (getTimeSeriesCategory() != null ? !getTimeSeriesCategory().equals(
                that.getTimeSeriesCategory()) : that.getTimeSeriesCategory() != null) {
            return false;
        }
        if (getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) :
				that.getOfficeId() != null) {
            return false;
        }
        if (getSharedAliasId() != null ? !getSharedAliasId().equals(
                that.getSharedAliasId()) : that.getSharedAliasId() != null) {
            return false;
        }
        if (getSharedRefTsId() != null ? !getSharedRefTsId().equals(
                that.getSharedRefTsId()) : that.getSharedRefTsId() != null) {
            return false;
        }
        return Objects.equals(assignedTimeSeries, that.assignedTimeSeries);
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getTimeSeriesCategory() != null ?
				getTimeSeriesCategory().hashCode() : 0);
        result = 31 * result + (getOfficeId() != null ? getOfficeId().hashCode() : 0);
        result = 31 * result + (getSharedAliasId() != null ? getSharedAliasId().hashCode() : 0);
        result = 31 * result + (getSharedRefTsId() != null ? getSharedRefTsId().hashCode() : 0);
        result = 31 * result + (assignedTimeSeries != null ? assignedTimeSeries.hashCode() : 0);
        return result;
    }

    @Override
    public void validate() throws FieldException {
        // Nothing to validate
    }
}
