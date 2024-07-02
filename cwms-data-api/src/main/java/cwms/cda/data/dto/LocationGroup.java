/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.csv.CsvV1;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;

@Schema(description = "A representation of a location group")
@JsonRootName("location_group")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.CSV, formatter = CsvV1.class)
public class LocationGroup extends CwmsDTO {
    private String id;
    private LocationCategory locationCategory;
    private String description;

    private String sharedLocAliasId;
    private String sharedRefLocationId;
    private Number locGroupAttribute;

    private List<AssignedLocation> assignedLocations = null;

    public LocationGroup() {
        // Should be unused
        super(null);
    }

    public LocationGroup(LocationCategory cat, String grpOfficeId, String grpId, String grpDesc,
                         String sharedLocAliasId, String sharedRefLocationId,
						 Number locGroupAttribute) {
        super(grpOfficeId);
        this.locationCategory = cat;
        this.id = grpId;
        this.description = grpDesc;
        this.sharedLocAliasId = sharedLocAliasId;
        this.sharedRefLocationId = sharedRefLocationId;
        this.locGroupAttribute = locGroupAttribute;
    }

    public LocationGroup(LocationGroup group, List<AssignedLocation> locs) {
        this(group);
        if (locs != null) {
            this.assignedLocations = new ArrayList<>();
            this.assignedLocations.addAll(locs);
        }
    }

    public LocationGroup(LocationGroup group) {
        super(group.getOfficeId());
        this.locationCategory = group.getLocationCategory();
        this.id = group.getId();
        this.description = group.getDescription();
        this.sharedLocAliasId = group.getSharedLocAliasId();
        this.sharedRefLocationId = group.getSharedRefLocationId();
        this.locGroupAttribute = group.getLocGroupAttribute();
        List<AssignedLocation> locs = group.getAssignedLocations();
        if (locs != null) {
            this.assignedLocations = new ArrayList<>();
            this.assignedLocations.addAll(locs);
        }
    }

    public String getId() {
        return id;
    }

    public LocationCategory getLocationCategory() {
        return locationCategory;
    }

    public String getDescription() {
        return description;
    }

    public String getSharedLocAliasId() {
        return sharedLocAliasId;
    }

    public String getSharedRefLocationId() {
        return sharedRefLocationId;
    }

    public Number getLocGroupAttribute() {
        return locGroupAttribute;
    }

    public List<AssignedLocation> getAssignedLocations() {
        return assignedLocations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LocationGroup that = (LocationGroup) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
            return false;
        }
        if (getLocationCategory() != null ? !getLocationCategory().equals(
                that.getLocationCategory()) : that.getLocationCategory() != null) {
            return false;
        }
        if (getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) :
				that.getOfficeId() != null) {
            return false;
        }
        if (getSharedLocAliasId() != null ? !getSharedLocAliasId().equals(
                that.getSharedLocAliasId()) : that.getSharedLocAliasId() != null) {
            return false;
        }
        if (getSharedRefLocationId() != null ? !getSharedRefLocationId().equals(
                that.getSharedRefLocationId()) : that.getSharedRefLocationId() != null) {
            return false;
        }
        if (getLocGroupAttribute() != null ? !getLocGroupAttribute().equals(
                that.getLocGroupAttribute()) : that.getLocGroupAttribute() != null) {
            return false;
        }
        return getAssignedLocations() != null ? getAssignedLocations().equals(
                that.getAssignedLocations()) : that.getAssignedLocations() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getLocationCategory() != null ? getLocationCategory().hashCode()
				: 0);
        result = 31 * result + (getOfficeId() != null ? getOfficeId().hashCode() : 0);
        result = 31 * result + (getSharedLocAliasId() != null ? getSharedLocAliasId().hashCode()
				: 0);
        result = 31 * result + (getSharedRefLocationId() != null ?
				getSharedRefLocationId().hashCode() : 0);
        result = 31 * result + (getLocGroupAttribute() != null ?
				getLocGroupAttribute().hashCode() : 0);
        result = 31 * result + (getAssignedLocations() != null ?
				getAssignedLocations().hashCode() : 0);
        return result;
    }
}
