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

    private final CwmsId projectId;

    @JsonIgnore
    private EnumMap<ProjectKind, List<CwmsId>> locationsByKind;

    private ProjectChildLocations(Builder builder) {
        this.projectId = builder.projectId;

        if (builder.locationsByKind != null) {
            this.locationsByKind = new EnumMap<>(builder.locationsByKind);
        }
    }

    public CwmsId getProjectId() {
        return projectId;
    }

    @JsonProperty
    public List<LocationsWithProjectKind> getLocationsByKind() {
        List<LocationsWithProjectKind> result = null;

        if (locationsByKind != null && !locationsByKind.isEmpty()) {
            result = new ArrayList<>();

            for (Map.Entry<ProjectKind, List<CwmsId>> entry : locationsByKind.entrySet()) {
                result.add(new LocationsWithProjectKind.Builder()
                        .withKind(entry.getKey())
                        .withLocationIds(entry.getValue()).build());
            }
        }

        return result;
    }

    public List<CwmsId> getLocationIds(ProjectKind kind) {
        if (locationsByKind != null && !locationsByKind.isEmpty()) {
            return locationsByKind.get(kind);
        }
        return null;
    }

    @JsonIgnore
    public List<CwmsId> getEmbankmentIds() {
        return getLocationIds(ProjectKind.EMBANKMENT);
    }

    @JsonIgnore
    public List<CwmsId> getLockIds() {
        return getLocationIds(ProjectKind.LOCK);
    }

    @JsonIgnore
    public List<CwmsId> getOutletIds() {
        return getLocationIds(ProjectKind.OUTLET);
    }

    @JsonIgnore
    public List<CwmsId> getTurbineIds() {
        return getLocationIds(ProjectKind.TURBINE);
    }

    @JsonIgnore
    public List<CwmsId> getGateIds() {
        return getLocationIds(ProjectKind.GATE);
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private CwmsId projectId;

        private EnumMap<ProjectKind, List<CwmsId>> locationsByKind;

        public Builder withProjectId(CwmsId projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withLocationsByKind(List<LocationsWithProjectKind> locationsByKind) {
            Builder retval = this;
            if (locationsByKind != null) {
                for (LocationsWithProjectKind item : locationsByKind) {
                    retval = retval.withLocationIds(item.getKind(), item.getLocationIds());
                }
            }

            return retval;
        }

        public Builder withLocationIds(ProjectKind kind, List<CwmsId> locationIds) {

            if (locationsByKind == null) {
                locationsByKind = new EnumMap<>(ProjectKind.class);
            }

            if (locationIds != null) {
                List<CwmsId> locOfKind = locationsByKind.computeIfAbsent(kind, k -> new ArrayList<>());
                if (!locationIds.isEmpty()) {
                    locOfKind.addAll(locationIds);
                }
            }

            return this;
        }

        @JsonIgnore
        public Builder withEmbankmentIds(List<CwmsId> embankmentIds) {
            return withLocationIds(ProjectKind.EMBANKMENT, embankmentIds);
        }

        @JsonIgnore
        public Builder withLockIds(List<CwmsId> lockIds) {
            return withLocationIds(ProjectKind.LOCK, lockIds);
        }

        @JsonIgnore
        public Builder withOutletIds(List<CwmsId> outletIds) {
            return withLocationIds(ProjectKind.OUTLET, outletIds);
        }

        @JsonIgnore
        public Builder withTurbineIds(List<CwmsId> turbineIds) {
            return withLocationIds(ProjectKind.TURBINE, turbineIds);
        }

        @JsonIgnore
        public Builder withGateIds(List<CwmsId> gateIds) {
            return withLocationIds(ProjectKind.GATE, gateIds);
        }

        public ProjectChildLocations build() {
            return new ProjectChildLocations(this);
        }
    }
}
