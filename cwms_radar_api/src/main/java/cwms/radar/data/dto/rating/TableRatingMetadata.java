package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = TableRatingMetadata.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TableRatingMetadata extends AbstractRatingMetadata {
    public static final String RATING_TYPE = "TABLE";

    private final String inRangeMethod;
    private final String outRangeLowMethod;
    private final String outRangeHighMethod;

    protected TableRatingMetadata(Builder builder) {
        super(builder);
        this.inRangeMethod = builder.inRangeMethod;
        this.outRangeLowMethod = builder.outRangeLowMethod;
        this.outRangeHighMethod = builder.outRangeHighMethod;
    }

    public String getInRangeMethod() {
        return inRangeMethod;
    }

    public String getOutRangeLowMethod() {
        return outRangeLowMethod;
    }

    public String getOutRangeHighMethod() {
        return outRangeHighMethod;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends AbstractRatingMetadata.Builder {
        private String inRangeMethod;
        private String outRangeLowMethod;
        private String outRangeHighMethod;


        public Builder() {
            super();
            this.ratingType = RATING_TYPE;
        }

        public Builder withInRangeMethod(String inRangeMethod) {
            this.inRangeMethod = inRangeMethod;
            return this;
        }


        public Builder withOutRangeLowMethod(String outRangeLowMethod) {
            this.outRangeLowMethod = outRangeLowMethod;
            return this;
        }

        public Builder withOutRangeHighMethod(String outRangeHighMethod) {
            this.outRangeHighMethod = outRangeHighMethod;
            return this;
        }

        public TableRatingMetadata build() {
            return new TableRatingMetadata(this);
        }
    }

}
