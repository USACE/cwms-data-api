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

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = Turbine.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({ "projectIdentifier", "location" })
public final class Turbine implements CwmsDTOBase {
    private final LocationIdentifier projectIdentifier;
    private final Location location;

    private Turbine(Builder builder) {
        this.projectIdentifier = builder.projectIdentifier;
        this.location = builder.location;
    }

    public LocationIdentifier getProjectIdentifier() {
        return projectIdentifier;
    }

    public Location getLocation() {
        return location;
    }

    @Override
    public void validate() throws FieldException {
        if (this.location == null) {
            throw new FieldException("Location field can't be null");
        }
        this.location.validate();
        if (this.projectIdentifier == null) {
            throw new FieldException("Project location Id field must be defined");
        }
        projectIdentifier.validate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Turbine turbine = (Turbine) o;
        return Objects.equals(getProjectIdentifier(), turbine.getProjectIdentifier())
                && Objects.equals(getLocation(), turbine.getLocation());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getProjectIdentifier());
        result = 31 * result + Objects.hashCode(getLocation());
        return result;
    }

    public static class Builder {
        private LocationIdentifier projectIdentifier;
        private Location location;

        public Builder withProjectIdentifier(LocationIdentifier projectIdentifier) {
            this.projectIdentifier = projectIdentifier;
            return this;
        }

        public Builder withLocation(Location location) {
            this.location = location;
            return this;
        }

        public Turbine build() {
            return new Turbine(this);
        }
    }
}
