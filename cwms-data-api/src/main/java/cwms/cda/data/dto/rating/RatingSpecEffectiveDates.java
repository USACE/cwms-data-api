package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = RatingSpecEffectiveDates.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class RatingSpecEffectiveDates extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String ratingSpecId;
    private final SortedSet<Instant> effectiveDates;

    public RatingSpecEffectiveDates(Builder builder) {
        this.ratingSpecId = builder.ratingSpecId;
        this.effectiveDates = builder.effectiveDates;
    }

    public String getRatingSpecId() {
        return ratingSpecId;
    }

    public SortedSet<Instant> getEffectiveDates() {
        return effectiveDates;
    }

    public static class Builder {
        private String ratingSpecId;
        private SortedSet<Instant> effectiveDates = new TreeSet<>();

        public Builder withRatingSpecId(String ratingSpecId) {
            this.ratingSpecId = ratingSpecId;
            return this;
        }

        public Builder withEffectiveDates(SortedSet<Instant> effectiveDates) {
            this.effectiveDates = effectiveDates;
            return this;
        }

        public RatingSpecEffectiveDates build() {
            return new RatingSpecEffectiveDates(this);
        }
    }
}
