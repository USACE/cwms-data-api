package cwms.cda.data.dto.timeSeriesText;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import hec.data.timeSeriesText.TextTimeSeriesRow;

@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries<T extends TextTimeSeriesRow>{


    private TextTimeSeries(Builder builder) {

    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        public Builder() {
        }

        public TextTimeSeries build(){
            return new TextTimeSeries(this);
        }
    }
}
