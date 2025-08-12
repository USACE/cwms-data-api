package cwms.cda.data.dto.rating;

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

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = RatingEffectiveDatesMap.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class RatingEffectiveDatesMap extends CwmsDTOBase {

    private final Map<String, List<RatingSpecEffectiveDates>> officeToSpecDates;

    public RatingEffectiveDatesMap(Builder builder) {
        this.officeToSpecDates = builder.officeToSpecDates;
    }

    public Map<String, List<RatingSpecEffectiveDates>> getOfficeToSpecDates() {
        return officeToSpecDates;
    }

    public static class Builder {
        private Map<String, List<RatingSpecEffectiveDates>> officeToSpecDates = new HashMap<>();

        public Builder withOfficeToSpecDates(Map<String, List<RatingSpecEffectiveDates>> officeToSpecDates) {
            this.officeToSpecDates = officeToSpecDates;
            return this;
        }

        public RatingEffectiveDatesMap build() {
            return new RatingEffectiveDatesMap(this);
        }
    }

}
