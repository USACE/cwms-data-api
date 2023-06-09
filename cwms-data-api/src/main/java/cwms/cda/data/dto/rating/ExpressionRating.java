package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema
@JsonDeserialize(builder = ExpressionRating.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ExpressionRating extends AbstractRatingMetadata {
    public static final String RATING_TYPE = "expression-rating";

    public String getExpression() {
        return expression;
    }

    private final String expression;

    protected ExpressionRating(Builder builder) {
        super(builder);
        this.expression = builder.expression;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends AbstractRatingMetadata.Builder {


        private String expression;

        public Builder() {
            super();
            this.ratingType = RATING_TYPE;
        }


        public ExpressionRating build() {
            return new ExpressionRating(this);
        }

        public AbstractRatingMetadata.Builder withExpression(String formula) {
            this.expression = formula;
            return this;
        }
    }

}
