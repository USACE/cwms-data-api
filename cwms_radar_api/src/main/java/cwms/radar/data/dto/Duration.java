package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = cwms.radar.data.dto.Duration.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Duration {

    private final int minutes;
    private final TimeOfRecord timeOfRecord;

    public Duration(Builder builder) {
        this.minutes = builder.minutes;
        this.timeOfRecord = builder.timeOfRecord;
    }

    public int getMinutes() {
        return minutes;
    }

    public TimeOfRecord getTimeOfRecord() {
        return timeOfRecord;
    }

    public enum TimeOfRecord {
        BOP, EOP
    }

    @Override
    public String toString() {
        // Probably need to have a name field and use that instead...
        if(TimeOfRecord.EOP.equals(timeOfRecord)){
            return String.format("%d", minutes);
        } else {
            return String.format("%d %s", minutes, timeOfRecord);
        }
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private int minutes;

        private TimeOfRecord timeOfRecord;

        public Builder() {
        }

        public Duration build() {
            return new Duration(this);
        }


        public Builder withMinutes(int min) {
            this.minutes = min;

            return this;
        }

        public Builder withDuration(mil.army.usace.hec.metadata.Duration duration) {
            Builder retval = this;

            retval = retval.withMinutes(duration.getMinutes());
            mil.army.usace.hec.metadata.Duration.TimeOfRecord tor =
                    duration.getTimeOfRecord();

            TimeOfRecord rec = TimeOfRecord.valueOf(tor.name());

            retval = retval.withTimeOfRecord(rec);

            return retval;
        }

        private Builder withTimeOfRecord(TimeOfRecord rec) {
            this.timeOfRecord = rec;
            return this;
        }
    }
}
