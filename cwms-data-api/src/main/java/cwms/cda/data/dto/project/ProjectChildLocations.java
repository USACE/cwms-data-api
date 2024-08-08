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

package cwms.cda.data.dto.project;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dao.project.ProjectKind;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds a project and lists of the project child locations by kind.
 */
@JsonDeserialize(builder = ProjectChildLocations.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, aliases = {Formats.JSON}, formatter = JsonV2.class)
public class ProjectChildLocations extends CwmsDTOBase {

    private final CwmsId project;

    @JsonIgnore
    private EnumMap<ProjectKind, List<CwmsId>> locationsByKind;

    private ProjectChildLocations(Builder builder) {
        this.project = builder.project;

        if (builder.locationsByKind != null) {
            this.locationsByKind = new EnumMap<>(builder.locationsByKind);
        }
    }

    public CwmsId getProject() {
        return project;
    }

    @JsonProperty
    public List<LocationsWithProjectKind> getLocationsByKind() {
        List<LocationsWithProjectKind> result = null;

        if (locationsByKind != null && !locationsByKind.isEmpty()) {
            result = new ArrayList<>();

            for (Map.Entry<ProjectKind, List<CwmsId>> entry : locationsByKind.entrySet()) {
                result.add(new LocationsWithProjectKind.Builder()
                        .withKind(entry.getKey())
                        .withLocations(entry.getValue()).build());
            }
        }

        return result;
    }

    public List<CwmsId> getLocations(ProjectKind kind) {
        if (locationsByKind != null && !locationsByKind.isEmpty()) {
            return locationsByKind.get(kind);
        }
        return null;
    }

    @JsonIgnore
    public List<CwmsId> getEmbankments() {
        return getLocations(ProjectKind.EMBANKMENT);
    }

    @JsonIgnore
    public List<CwmsId> getLocks() {
        return getLocations(ProjectKind.LOCK);
    }

    @JsonIgnore
    public List<CwmsId> getOutlets() {
        return getLocations(ProjectKind.OUTLET);
    }

    @JsonIgnore
    public List<CwmsId> getTurbines() {
        return getLocations(ProjectKind.TURBINE);
    }

    @JsonIgnore
    public List<CwmsId> getGates() {
        return getLocations(ProjectKind.GATE);
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private CwmsId project;

        private EnumMap<ProjectKind, List<CwmsId>> locationsByKind;

        public Builder withProject(CwmsId project) {
            this.project = project;
            return this;
        }

        public Builder withLocationsByKind(List<LocationsWithProjectKind> locationsByKind) {
            Builder retval = this;
            if (locationsByKind != null) {
                for (LocationsWithProjectKind item : locationsByKind) {
                    retval = retval.withLocations(item.getKind(), item.getLocations());
                }
            }

            return retval;
        }

        public Builder withLocations(ProjectKind kind, List<CwmsId> locations) {

            if (locationsByKind == null) {
                locationsByKind = new EnumMap<>(ProjectKind.class);
            }

            if (locations != null) {
                List<CwmsId> locOfKind = locationsByKind.computeIfAbsent(kind, k -> new ArrayList<>());
                if (!locations.isEmpty()) {
                    locOfKind.addAll(locations);
                }
            }

            return this;
        }

        @JsonIgnore
        public Builder withEmbankments(List<CwmsId> embankments) {
            return withLocations(ProjectKind.EMBANKMENT, embankments);
        }

        @JsonIgnore
        public Builder withLocks(List<CwmsId> locks) {
            return withLocations(ProjectKind.LOCK, locks);
        }

        @JsonIgnore
        public Builder withOutlets(List<CwmsId> outlets) {
            return withLocations(ProjectKind.OUTLET, outlets);
        }

        @JsonIgnore
        public Builder withTurbines(List<CwmsId> turbines) {
            return withLocations(ProjectKind.TURBINE, turbines);
        }

        @JsonIgnore
        public Builder withGates(List<CwmsId> gates) {
            return withLocations(ProjectKind.GATE, gates);
        }

        public ProjectChildLocations build() {
            return new ProjectChildLocations(this);
        }
    }
}
