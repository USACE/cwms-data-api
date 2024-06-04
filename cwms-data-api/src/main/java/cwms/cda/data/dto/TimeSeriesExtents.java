package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@JsonRootName("extents")
@Schema(description = "TimeSeries extent information")
@JsonPropertyOrder(alphabetic = true)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesExtents {

    @Schema(description = "TimeSeries version to which this extent information applies")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime versionTime;

    @Schema(description = "Earliest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime earliestTime;

    @Schema(description = "Latest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime latestTime;

    @Schema(description = "Last update in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime lastUpdate;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private TimeSeriesExtents() {
    }

    public TimeSeriesExtents(final ZonedDateTime versionTime, final ZonedDateTime earliestTime,
                             final ZonedDateTime latestTime, final ZonedDateTime lastUpdateTime) {
        this.versionTime = versionTime;
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
        this.lastUpdate = lastUpdateTime;
    }

    public TimeSeriesExtents(final Timestamp versionTime, final Timestamp earliestTime,
                             final Timestamp latestTime, final Timestamp lastUpdateTime) {
        this(toZdt(versionTime), toZdt(earliestTime), toZdt(latestTime), toZdt(lastUpdateTime));
    }

    private static ZonedDateTime toZdt(final Timestamp time) {
        if (time != null) {
            return ZonedDateTime.ofInstant(time.toInstant(), ZoneId.of("UTC"));
        } else {
            return null;
        }
    }

    public ZonedDateTime getVersionTime() {
        return this.versionTime;
    }

    public ZonedDateTime getEarliestTime() {
        return this.earliestTime;
    }

    public ZonedDateTime getLatestTime() {
        return this.latestTime;
    }

    public ZonedDateTime getLastUpdate() {
        return this.lastUpdate;
    }

}
