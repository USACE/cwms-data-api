package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Entity.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Entity extends CwmsDTOBase {

    @JsonProperty(required = true)
    private final CwmsId id;
    private final String parentEntityId;
    private final String categoryId;
    private final String longName;

    private Entity(Builder builder) {
        this.id = builder.id;
        this.parentEntityId = builder.parentEntityId;
        this.categoryId = builder.categoryId;
        this.longName = builder.longName;
    }

    public CwmsId getId() {
        return id;
    }

    public String getParentEntityId() {
        return parentEntityId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public String getLongName() {
        return longName;
    }

    public static final class Builder {
        private CwmsId id;
        private String parentEntityId;
        private String categoryId;
        private String longName;

        public Builder withId(CwmsId id) {
            this.id = id;
            return this;
        }

        public Builder withParentEntityId(String parentEntityId) {
            this.parentEntityId = parentEntityId;
            return this;
        }

        public Builder withCategoryId(String categoryId) {
            this.categoryId = categoryId;
            return this;
        }

        public Builder withLongName(String longName) {
            this.longName = longName;
            return this;
        }

        public Entity build() {
            return new Entity(this);
        }
    }
}
