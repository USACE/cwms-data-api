/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public abstract class ProjectStructure implements CwmsDTOBase {
    private final CwmsId projectId;
    private final Location location;

    protected ProjectStructure(CwmsId projectId, Location location) {
        this.location = location;
        this.projectId = projectId;
    }

    public final CwmsId getProjectId() {
        return projectId;
    }

    public final Location getLocation() {
        return location;
    }

    protected final List<String> getMissingFields() {
        List<String> output = new ArrayList<>();

        if (getLocation() == null) {
            output.add("Location");
        } else {
            try {
                getLocation().validate();
            } catch (RequiredFieldException ex) {
                output.addAll(ex.getDetails().get(RequiredFieldException.MISSING_FIELDS));
            }
        }
        if (getProjectId() == null) {
            output.add("ProjectId");
        } else {
            try {
                getProjectId().validate();
            } catch (RequiredFieldException ex) {
                output.addAll(ex.getDetails().get(RequiredFieldException.MISSING_FIELDS));
            }
        }

        return output;
    }
}
