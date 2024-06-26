package cwms.cda.data.dto.timeseriesprofile;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfile.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfile extends CwmsDTO {
    @Schema(description = "Location ID")
    // TODO replace with CWMSID
    private final String locationId;
    @Schema(description = "Description")
    private final String description;
    @Schema(description = "Parameter List")
    private final List<String> parameterList;
    @Schema(description = "Key Parameter")
    private final String keyParameter;
    @Schema(description = "Reference TS")
    // TODO replace with CWMSID
    private final String refTsId;

    private TimeSeriesProfile(Builder builder) {
        super(builder.officeId);
        this.locationId = builder.locationId;
        this.description = builder.description;
        this.keyParameter = builder.keyParameter;
        this.parameterList = builder.parameterList;
        this.refTsId = builder.refTsId;
    }

    public String getLocationId() {
        return locationId;
    }

    public String getDescription() {
        return description;
    }

    public String getKeyParameter() {
        return keyParameter;
    }

    public List<String> getParameterList() {
        return parameterList != null ? new ArrayList<>(parameterList) : null;
    }

    public String getRefTsId() {
        return refTsId;
    }

    @Override
    public void validate() throws FieldException {
        if (this.parameterList == null) {
            throw new FieldException("Parameter list field can't be null");
        }
        if (this.keyParameter == null) {
            throw new FieldException("Key Parameter field can't be null");
        }
        if (this.officeId == null) {
            throw new FieldException("Office Id field can't be null");
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
        private String officeId;
        private List<String> parameterList;
        private String keyParameter;
        private String description;
        private String locationId;
        private String refTsId;

        public TimeSeriesProfile.Builder withLocationId(String locationId) {
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
            this.parameterList = parameterList != null ? new ArrayList<>(parameterList) : null;
            return this;
        }

        public TimeSeriesProfile.Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public TimeSeriesProfile.Builder withRefTsId(String refTsId) {
            this.refTsId = refTsId;
            return this;
        }


        public TimeSeriesProfile build() {
            return new TimeSeriesProfile(this);
        }
    }
}
