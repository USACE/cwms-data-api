package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
        subTypes = {
                UsgsStreamRating.class
        }
)
@JsonDeserialize(builder = TableRating.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class TableRating extends AbstractRatingMetadata {
    public static final String RATING_TYPE = "table";

    private final String inRangeMethod;
    private final String outRangeLowMethod;
    private final String outRangeHighMethod;

    protected TableRating(Builder builder) {
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

        public TableRating build() {
            return new TableRating(this);
        }
    }

}
