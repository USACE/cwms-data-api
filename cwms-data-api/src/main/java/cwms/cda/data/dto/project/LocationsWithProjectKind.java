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
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.List;

@JsonDeserialize(builder = LocationsWithProjectKind.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, aliases = {Formats.JSON}, formatter = JsonV1.class)
public class LocationsWithProjectKind {
    ProjectKind kind;
    List<CwmsId> locationIds;

    private LocationsWithProjectKind(Builder builder) {
        kind = builder.kind;
        locationIds = builder.locationIds;
    }

    public ProjectKind getKind() {
        return kind;
    }

    public List<CwmsId> getLocationIds() {
        return locationIds;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private ProjectKind kind;
        private List<CwmsId> locationIds = new ArrayList<>();

        public Builder() {

        }

        public Builder withKind(ProjectKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder withLocationIds(List<CwmsId> locationIds) {
            if (locationIds == null) {
                this.locationIds = null;
            } else {
                this.locationIds = new ArrayList<>(locationIds);
            }
            return this;
        }

        public LocationsWithProjectKind build() {
            return new LocationsWithProjectKind(this);
        }
    }
}
