package cwms.cda.data.dto.timeseriesprofile;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfile.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfile extends CwmsDTOBase {
    @Schema(description = "Location ID")
    private final CwmsId locationId;
    @Schema(description = "Description")
    private final String description;
    @Schema(description = "Parameter List")
    private final List<String> parameterList;
    @Schema(description = "Key Parameter")
    private final String keyParameter;
    @Schema(description = "Reference TS")
    private final CwmsId referenceTsId;

    private TimeSeriesProfile(Builder builder) {
        this.locationId = builder.locationId;
        this.description = builder.description;
        this.keyParameter = builder.keyParameter;
        this.parameterList = builder.parameterList;
        this.referenceTsId = builder.referenceTsId;
    }

    public CwmsId getLocationId() {
        return locationId;
    }

    public String getDescription() {
        return description;
    }

    public String getKeyParameter() {
        return keyParameter;
    }

    public List<String> getParameterList() {
        return parameterList;
    }

    public CwmsId getReferenceTsId() {
        return referenceTsId;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator){
        super.validateInternal(validator);

        if (this.parameterList.isEmpty()) {
            throw new FieldException("Parameter list field must not be empty");
        }
        if (this.keyParameter == null) {
            throw new FieldException("Key Parameter field can't be null");
        }
        if (this.locationId == null) {
            throw new FieldException("Location Id field can't be null");
        }
        if (!parameterList.contains(keyParameter)) {
            throw new FieldException("Key Parameter must be part of Parameter list");
        }
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private final List<String> parameterList = new ArrayList<>();
        private String keyParameter;
        private String description;
        private CwmsId locationId;
        private CwmsId referenceTsId;

        public TimeSeriesProfile.Builder withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return this;
        }

        public TimeSeriesProfile.Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public TimeSeriesProfile.Builder withKeyParameter(String keyParameter) {
            this.keyParameter = keyParameter;
            return this;
        }

        public TimeSeriesProfile.Builder withParameterList(List<String> parameterList) {
            this.parameterList.clear();
            if (parameterList != null) {
                this.parameterList.addAll(parameterList);
            }
             return this;
        }

        public TimeSeriesProfile.Builder withReferenceTsId(CwmsId referenceTsId) {
            this.referenceTsId = referenceTsId;
            return this;
        }


        public TimeSeriesProfile build() {
            return new TimeSeriesProfile(this);
        }
    }
}
