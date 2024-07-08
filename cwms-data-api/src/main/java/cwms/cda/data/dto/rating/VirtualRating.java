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
import java.util.ArrayList;
import java.util.List;

@Schema
@JsonDeserialize(builder = VirtualRating.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class VirtualRating extends AbstractRatingMetadata {
    public static final String RATING_TYPE = "virtual";


    private final List<SourceRating> sourceRatings;
    private String connections;


    protected VirtualRating(Builder builder) {
        super(builder);
        this.connections = builder.connections;
        if(builder.sourceRatings != null) {
            this.sourceRatings = new ArrayList<>(builder.sourceRatings);
        } else {
            this.sourceRatings = null;
        }

    }
    public List<SourceRating> getSourceRatings() {
        return sourceRatings;
    }

    public String getConnections() {
        return connections;
    }

    public static class SourceRating {
         String ratingSpecId = null;
         String ratingExpression = null;
         Integer position = null;
    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder extends AbstractRatingMetadata.Builder {

        List<SourceRating> sourceRatings = null;
        String connections = null;

        public Builder() {
            super();
            this.ratingType = RATING_TYPE;
        }


        public Builder withSourceRating(SourceRating sourceRating) {
            if (sourceRating != null) {
                if (this.sourceRatings == null) {
                    this.sourceRatings = new ArrayList<>();
                }
                this.sourceRatings.add(sourceRating);
            }
            return this;
        }

        public Builder withConnections(String connections) {
            this.connections = connections;
            return this;
        }

        public VirtualRating build() {
            return new VirtualRating(this);
        }
    }

}
