package cwms.cda.data.dto.csv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;

import java.time.Instant;

/**
 * Single DTO for TimeSeries CSV rows. All potential columns exist on this class;
 * only date-time and value are considered required and are annotated with @CsvRequiredColumn
 * so that when using the CsvV1 mapper they are the only columns included. All other
 * fields are considered optional and will only be included if the CsvConfiguration specified including them
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonDeserialize(builder = TimeSeriesCsvRow.Builder.class)
public final class TimeSeriesCsvRow extends CwmsDTOBase {

    @CsvRequiredColumn
    @JsonProperty(index = 0)
    private final Instant dateTime;

    @CsvRequiredColumn
    @JsonProperty(index = 1)
    private final Double value;

    @JsonProperty(index = 2)
    private final Instant dataEntryDate;

    @JsonProperty(index = 3)
    private final Integer qualityCode;

    @CsvUnitHeader(field = "value")
    private final String units;

    private TimeSeriesCsvRow(Builder builder) {
        this.dateTime = builder.dateTime;
        this.value = builder.value;
        this.qualityCode = builder.qualityCode;
        this.dataEntryDate = builder.dataEntryDate;
        this.units = builder.units;
    }

    public Instant getDateTime() { return dateTime; }
    public Double getValue() { return value; }
    public Integer getQualityCode() { return qualityCode; }
    public Instant getDataEntryDate() { return dataEntryDate; }
    public String getUnits() { return units; }

    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private Instant dateTime;
        private Double value;
        private Instant dataEntryDate;
        private Integer qualityCode;
        private String units;

        public Builder withDateTime(Instant dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withValue(Double value) {
            this.value = value;
            return this;
        }

        public Builder withDataEntryDate(Instant dataEntryDate) {
            this.dataEntryDate = dataEntryDate;
            return this;
        }

        public Builder withQualityCode(Integer qualityCode) {
            this.qualityCode = qualityCode;
            return this;
        }

        public Builder withUnits(String units) {
            this.units = units;
            return this;
        }

        public TimeSeriesCsvRow build() {
            return new TimeSeriesCsvRow(this);
        }
    }
}
