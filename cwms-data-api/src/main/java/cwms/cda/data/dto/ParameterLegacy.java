package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

/**
 * DTO used exclusively for legacy JSON (version 1) parameter payloads.
 * The property names follow kebab-case to match the legacy API fields.
 */
@FormattableWith(contentType = Formats.JSON_LEGACY, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON, Formats.JSONV1})
@JsonRootName("parameter")
@JsonDeserialize(builder = ParameterLegacy.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class ParameterLegacy extends CwmsDTOBase {

    private final String abstractParam;
    private final String name;
    private final String office;
    private final String defaultEnglishUnit;
    private final String defaultSiUnit;
    private final String longName;
    private final String description;

    private ParameterLegacy(Builder builder) {
        this.abstractParam = builder.abstractParam;
        this.name = builder.name;
        this.office = builder.office;
        this.defaultEnglishUnit = builder.defaultEnglishUnit;
        this.defaultSiUnit = builder.defaultSiUnit;
        this.longName = builder.longName;
        this.description = builder.description;
    }

    public String getAbstractParam() {
        return abstractParam;
    }

    public String getName() {
        return name;
    }

    public String getOffice() {
        return office;
    }

    public String getDefaultEnglishUnit() {
        return defaultEnglishUnit;
    }

    public String getDefaultSiUnit() {
        return defaultSiUnit;
    }

    public String getLongName() {
        return longName;
    }

    public String getDescription() {
        return description;
    }

    public static class Builder {
        private String abstractParam;
        private String name;
        private String office;
        private String defaultEnglishUnit;
        private String defaultSiUnit;
        private String longName;
        private String description;

        public Builder withAbstractParam(String abstractParam) {
            this.abstractParam = abstractParam;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withOffice(String office) {
            this.office = office;
            return this;
        }

        public Builder withDefaultEnglishUnit(String defaultEnglishUnit) {
            this.defaultEnglishUnit = defaultEnglishUnit;
            return this;
        }

        public Builder withDefaultSiUnit(String defaultSiUnit) {
            this.defaultSiUnit = defaultSiUnit;
            return this;
        }

        public Builder withLongName(String longName) {
            this.longName = longName;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        @JsonIgnore
        public Builder from(ParameterLegacy parameter) {
            this.abstractParam = parameter.getAbstractParam();
            this.name = parameter.getName();
            this.office = parameter.getOffice();
            this.defaultEnglishUnit = parameter.getDefaultEnglishUnit();
            this.defaultSiUnit = parameter.getDefaultSiUnit();
            this.longName = parameter.getLongName();
            this.description = parameter.getDescription();
            return this;
        }

        public ParameterLegacy build() {
            return new ParameterLegacy(this);
        }
    }
}
