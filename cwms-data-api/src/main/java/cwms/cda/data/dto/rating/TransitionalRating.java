package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;

@Schema
@JsonDeserialize(builder = TransitionalRating.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TransitionalRating extends AbstractRatingMetadata {
    public static final String RATING_TYPE = "transitional";

    private final List<String> sourceRatings;
    private final List<String> conditions;
    private final List<String> evaluations;


    protected TransitionalRating(Builder builder) {
        super(builder);
        if(builder.sourceRatingIds != null) {
            this.sourceRatings = new ArrayList<>(builder.sourceRatingIds);
        } else {
            this.sourceRatings = null;
        }
        if(builder.conditions != null) {
            this.conditions = new ArrayList<>(builder.conditions);
        } else {
            this.conditions = null;
        }
        if(builder.evaluations != null) {
            this.evaluations = new ArrayList<>(builder.evaluations);
        } else {
            this.evaluations = null;
        }

    }

    public List<String> getSourceRatings() {
        return sourceRatings;
    }

    public List<String> getConditions() {
        return conditions;
    }

    public List<String> getEvaluations() {
        return evaluations;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends AbstractRatingMetadata.Builder {
        private List<String> sourceRatingIds;
        private List<String> conditions;
        private List<String> evaluations;


        public Builder() {
            super();
            this.ratingType = RATING_TYPE;
        }

        public Builder withSourceRatingIds(List<String> sourceRatingIds) {
            this.sourceRatingIds = sourceRatingIds;
            return this;
        }

        public Builder withConditions(List<String> conditions) {
            this.conditions = conditions;
            return this;
        }

        public Builder withEvaluations(List<String> evaluations) {
            this.evaluations = evaluations;
            return this;
        }

        public TransitionalRating build() {
            return new TransitionalRating(this);
        }
    }

}
