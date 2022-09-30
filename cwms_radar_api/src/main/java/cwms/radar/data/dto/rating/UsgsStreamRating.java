package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = UsgsStreamRating.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class UsgsStreamRating extends SimpleRating {
    public static final String RATING_TYPE = "USGS";


    private final String usgsSite;

    protected UsgsStreamRating(Builder builder) {
        super(builder);
        this.usgsSite = builder.usgsSite;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends SimpleRating.Builder {


        private String usgsSite;

        public Builder() {
            super();
            this.ratingType = RATING_TYPE;
        }


        public UsgsStreamRating build() {
            return new UsgsStreamRating(this);
        }

        public AbstractRatingMetadata.Builder withUsgsSite(String usgsSite) {
            this.usgsSite = usgsSite;
            return this;
        }
    }

}
