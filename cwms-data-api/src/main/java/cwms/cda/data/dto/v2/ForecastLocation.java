package cwms.cda.data.dto.v2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonDeserialize(builder = ForecastLocation.Builder.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class, aliases = {Formats.DEFAULT})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ForecastLocation extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String locationId;

    @JsonProperty(required = true)
    @Schema(example = "-1")
    private final Integer sortOrder;

    @JsonProperty("is-primary")
    @Schema(example = "true")
    private final Boolean isPrimary;

    private ForecastLocation(Builder builder) {
        this.locationId = builder.locationId;
        this.sortOrder = builder.sortOrder;
        this.isPrimary = builder.isPrimary;
    }

    public String getLocationId() {
        return locationId;
    }

    public Integer getSortOrder() {
        return sortOrder;
    }

    @JsonProperty("is-primary")
    public Boolean isPrimary() {
        return isPrimary;
    }

    public static final class Builder {
        private String locationId;
        private Integer sortOrder;
        private Boolean isPrimary;

        public Builder() {
        }

        public Builder withLocationId(String locationId) {
            this.locationId = locationId;
            return this;
        }

        public Builder withSortOrder(Integer sortOrder) {
            if(sortOrder != null) {
                if(sortOrder == -1) {
                    if(isPrimary == null) {
                        this.isPrimary = true;
                    } else if(!isPrimary) {
                        throw new IllegalArgumentException("isPrimary must be true if sortOrder is -1");
                    }
                } else {
                    if(isPrimary == null) {
                        this.isPrimary = false;
                    } else if(isPrimary) {
                        throw new IllegalArgumentException("isPrimary must be false if sortOrder is not -1");
                    }
                }
            }
            this.sortOrder = sortOrder;
            return this;
        }

        @JsonProperty("is-primary")
        public Builder withIsPrimary(Boolean isPrimary) {
            if(isPrimary != null) {
                if(isPrimary) {
                    if(this.sortOrder == null) {
                        this.sortOrder = -1;
                    } else if(this.sortOrder != -1) {
                        throw new IllegalArgumentException("sortOrder must be -1 if isPrimary is true");
                    }
                } else {
                    if(this.sortOrder != null && this.sortOrder == -1) {
                        throw new IllegalArgumentException("sortOrder cannot be -1 if isPrimary is false");
                    }
                }
            }
            this.isPrimary = isPrimary;
            return this;
        }

        public ForecastLocation build() {
            return new ForecastLocation(this);
        }
    }

}
