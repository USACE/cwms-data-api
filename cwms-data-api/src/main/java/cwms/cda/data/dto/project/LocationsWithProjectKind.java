package cwms.cda.data.dto.project;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dao.project.ProjectKind;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.List;

@JsonDeserialize(builder = LocationsWithProjectKind.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, aliases = {Formats.JSON}, formatter = JsonV2.class)
public class LocationsWithProjectKind {
    ProjectKind kind;
    List<CwmsId> locations;

    private LocationsWithProjectKind(Builder builder) {
        kind = builder.kind;
        locations = builder.locations;
    }

    public ProjectKind getKind() {
        return kind;
    }

    public List<CwmsId> getLocations() {
        return locations;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private ProjectKind kind;
        private List<CwmsId> locations = new ArrayList<>();

        public Builder() {

        }

        public Builder withKind(ProjectKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder withLocations(List<CwmsId> locations) {
            if (locations == null) {
                this.locations = null;
            } else {
                this.locations = new ArrayList<>(locations);
            }
            return this;
        }

        public LocationsWithProjectKind build() {
            return new LocationsWithProjectKind(this);
        }
    }
}
