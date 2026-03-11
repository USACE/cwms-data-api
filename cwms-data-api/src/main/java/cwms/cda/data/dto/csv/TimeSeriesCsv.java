package cwms.cda.data.dto.csv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.csv.CsvMetadata;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Single DTO for TimeSeries CSV rows. All potential columns exist on this class;
 * only date-time and value are considered required and are annotated with @CsvRow
 * so that when using the CsvV1 mapper they are the only columns included. All other
 * fields are annotated with @CsvMetadata and will only be serialized when using a
 * CsvMapper that does not apply CsvRow filtering (e.g., for metadata-as-columns=true).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"time-series-id", "office-id", "date-time", "value", "units", "version-date", "quality-code"})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesCsv extends CwmsDTOBase {

    @CsvMetadata
    @JsonProperty(value = "time-series-id", index = 0)
    private final String timeSeriesId;

    @CsvMetadata
    @JsonProperty(value = "office-id", index = 1)
    private final String officeId;

    @JsonProperty(value = "date-time", index = 2)
    private final Instant dateTime;

    @JsonProperty(value = "value", index = 3)
    private final Double value;

    @CsvMetadata
    @JsonProperty(value = "units", index = 4)
    private final String units;

    @CsvMetadata
    @JsonProperty(value = "version-date", index = 5)
    private final Instant versionDate;

    @CsvMetadata
    @JsonProperty(value = "quality-code", index = 6)
    private final Integer qualityCode;

    public TimeSeriesCsv(String timeSeriesId, String officeId, Instant dateTime, Double value,
                         String units, Instant versionDate, Integer qualityCode) {
        this.timeSeriesId = timeSeriesId;
        this.officeId = officeId;
        this.dateTime = dateTime;
        this.value = value;
        this.units = units;
        this.versionDate = versionDate;
        this.qualityCode = qualityCode;
    }

    public String getTimeSeriesId() { return timeSeriesId; }
    public String getOfficeId() { return officeId; }
    public Instant getDateTime() { return dateTime; }
    public Double getValue() { return value; }
    public String getUnits() { return units; }
    public Instant getVersionDate() { return versionDate; }
    public Integer getQualityCode() { return qualityCode; }
}
