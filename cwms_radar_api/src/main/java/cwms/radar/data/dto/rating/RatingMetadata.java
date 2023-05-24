package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.CwmsDTOBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@JsonDeserialize(builder = RatingMetadata.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RatingMetadata implements CwmsDTOBase {

    private final RatingSpec ratingSpec;

    private final List<AbstractRatingMetadata> ratings;

    private RatingMetadata(Builder builder) {
        this.ratingSpec = builder.ratingSpec;
        this.ratings = builder.ratings;
    }

    @Override
    public void validate() throws FieldException {

    }

    public RatingSpec getRatingSpec() {
        return ratingSpec;
    }

    public List<AbstractRatingMetadata> getRatings() {
        return ratings;
    }

    @JsonIgnore
    public int getSize() {
        int retval = 0;
        if (ratings != null) {
            retval = ratings.size();
        }
        return retval;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private RatingSpec ratingSpec;
        List<AbstractRatingMetadata> ratings;

        public Builder() {
        }


        public Builder withRatingSpec(RatingSpec ratingSpec) {
            this.ratingSpec = ratingSpec;
            return this;
        }

        public Builder withRatings(Collection<AbstractRatingMetadata> ratings) {
            this.ratings = new ArrayList<>(ratings);
            return this;
        }


        public RatingMetadata build() {

            if (ratingSpec != null && ratings != null) {
                // Not putting the effective dates in the spec because we are
                // only fetching a page of the ratings and may not have the complete
                // list of ratings contained by the spec.
//                List<ZonedDateTime> effectiveZdts = ratings.stream().map(r -> r
//                .getEffectiveDate())
//                        .collect(Collectors.toList());

                ratingSpec = new RatingSpec.Builder().fromRatingSpec(ratingSpec)
//                        .withEffectiveDates(effectiveZdts)
                        .build();
            }

            return new RatingMetadata(this);
        }


    }
}
