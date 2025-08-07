package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = RatingEffectiveDatesMap.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class RatingEffectiveDatesMap extends CwmsDTOBase {
    private final Map<String, List<RatingSpecEffectiveDates>> officeToSpecDatesMap;

    public RatingEffectiveDatesMap(Builder builder) {
        this.officeToSpecDatesMap = builder.officeToSpecDatesMap;
    }

    @JsonAnyGetter
    public Map<String, List<RatingSpecEffectiveDates>> getOfficeToSpecDatesMap() {
        return officeToSpecDatesMap;
    }

    public static class Builder {
        private Map<String, List<RatingSpecEffectiveDates>> officeToSpecDatesMap = new HashMap<>();

        @JsonAnySetter
        public Builder withOfficeToSpecDates(String officeId, List<RatingSpecEffectiveDates> specDates) {
            this.officeToSpecDatesMap.put(officeId, specDates);
            return this;
        }

        public Builder withOfficeToSpecDatesMap(Map<String, List<RatingSpecEffectiveDates>> officeToSpecDatesMap) {
            this.officeToSpecDatesMap = officeToSpecDatesMap;
            return this;
        }

        public RatingEffectiveDatesMap build() {
            return new RatingEffectiveDatesMap(this);
        }
    }
}
