package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfile.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfile extends CwmsDTOBase {
    @Schema(description = "Location ID")
    private final CwmsId locationId;
    @Schema(description = "Description")
    private final String description;
    @Schema(description = "Dependent Parameters")
    private final List<String> parameterList;
    @Schema(description = "Independent Parameter")
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
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.required(getParameterList(), "parameterList");
        validator.required(getKeyParameter(), "keyParameter");
        validator.required(getLocationId(), "locationId");
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
