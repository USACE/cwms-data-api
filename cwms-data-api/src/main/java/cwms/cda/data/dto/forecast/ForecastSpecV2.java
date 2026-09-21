package cwms.cda.data.dto.forecast;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.List;

@JsonRootName("forecast-spec")
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class, aliases = {Formats.DEFAULT})
@JsonDeserialize(builder = ForecastSpecV2.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ForecastSpecV2 extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final CwmsId specId;
    private final String designator;
    private final List<ForecastLocation> locationIds;
    private final String sourceEntityId;
    private final String description;
    private final List<String> timeSeriesIds;


    private ForecastSpecV2(Builder builder) {
        this.specId = builder.specId;
        this.designator = builder.designator;
        this.locationIds = builder.locationIds;
        this.sourceEntityId = builder.sourceEntityId;
        this.description = builder.description;
        this.timeSeriesIds = builder.timeSeriesIds;
    }

    public CwmsId getSpecId() {
        return specId;
    }

    public List<ForecastLocation> getLocationIds() {
        return locationIds;
    }

    public String getSourceEntityId() {
        return sourceEntityId;
    }

    public String getDesignator() {
        return designator;
    }

    public String getDescription() {
        return description;
    }

    public List<String> getTimeSeriesIds() {
        return timeSeriesIds;
    }

    public static class Builder {
        private CwmsId specId;
        private String designator;
        private List<ForecastLocation> locationIds;
        private String sourceEntityId;
        private String description;
        private List<String> timeSeriesIds;

        public Builder() {

        }

        public Builder withSpecId(CwmsId specId) {
            this.specId = specId;
            return this;
        }

        public Builder withDesignator(String designator) {
            this.designator = designator;
            return this;
        }

        public Builder withLocationIds(List<ForecastLocation> locationIds) {
            this.locationIds = locationIds;
            return this;
        }

        public Builder withSourceEntityId(String sourceEntityId) {
            this.sourceEntityId = sourceEntityId;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withTimeSeriesIds(List<String> timeSeriesIds) {
            this.timeSeriesIds = timeSeriesIds;
            return this;
        }

        @JsonIgnore
        public Builder from(ForecastSpecV2 forecastSpec) {
            this.specId = forecastSpec.getSpecId();
            this.designator = forecastSpec.getDesignator();
            this.locationIds = forecastSpec.getLocationIds();
            this.sourceEntityId = forecastSpec.getSourceEntityId();
            this.description = forecastSpec.getDescription();
            this.timeSeriesIds = forecastSpec.getTimeSeriesIds();
            return this;
        }

        public ForecastSpecV2 build() {
            return new ForecastSpecV2(this);
        }
    }

}
