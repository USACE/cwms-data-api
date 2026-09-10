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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class AssignedTimeSeries extends CwmsDTOBase {
    private String officeId;
    private String timeseriesId;
    private String aliasId;
    private String refTsId;
    private Integer attribute;
    @Schema(description = "Optional persistent display units override for this time series, shared across groups. "
        + "Omit to preserve the current preference; set null to clear it.", example = "W/m2", nullable = true)
    private String units;
    @JsonIgnore
    private boolean unitsSpecified;
    @Schema(description = "Unit system for the display units override (EN or SI). Defaults to EN on writes.",
        allowableValues = {"EN", "SI"}, example = "EN")
    private String unitSystem;

    public AssignedTimeSeries() {

    }


    public AssignedTimeSeries(String officeId, String timeseriesId,
                              String aliasId, String refTsId, Integer attr) {
        this.officeId = officeId;
        this.timeseriesId = timeseriesId;
        this.aliasId = aliasId;
        this.refTsId = refTsId;
        this.attribute = attr;
    }

    public String getOfficeId() {
        return officeId;
    }

    public AssignedTimeSeries(String officeId, String timeseriesId,
                              String aliasId, String refTsId, Integer attr, String units, String unitSystem) {
        this(officeId, timeseriesId, aliasId, refTsId, attr);
        this.units = units;
        this.unitsSpecified = units != null;
        this.unitSystem = unitSystem;
    }

    public String getUnits() {
        return units;
    }

    @JsonSetter("units")
    public void setUnits(String units) {
        this.units = units;
        this.unitsSpecified = true;
    }

    @JsonIgnore
    public boolean isUnitsSpecified() {
        return unitsSpecified;
    }

    public String getUnitSystem() {
        return unitSystem;
    }

    public String getTimeseriesId() {
        return timeseriesId;
    }

    public String getAliasId() {
        return aliasId;
    }

    public String getRefTsId() {
        return refTsId;
    }

    public Integer getAttribute() {
        return attribute;
    }
}
