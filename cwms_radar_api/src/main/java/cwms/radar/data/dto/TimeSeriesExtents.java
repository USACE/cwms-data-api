package cwms.radar.data.dto;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;

import org.apache.commons.io.comparator.LastModifiedFileComparator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import cwms.radar.data.dto.TimeSeries.Record;
import cwms.radar.formatters.xml.adapters.DurationAdapter;
import cwms.radar.formatters.xml.adapters.TimestampAdapter;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.AccessMode;

@XmlRootElement(name = "extents")
@Schema(description = "TimeSeries extent information")
@XmlSeeAlso(Record.class)
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
@JsonPropertyOrder(alphabetic = true)
public class TimeSeriesExtents {
    //public static final String ZONED_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ'['VV']'";

    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @Schema(description = "TimeSeries version to which this extent information applies")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime versionTime;

    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @Schema(description = "Earliest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    @XmlElement(name = "earliest-time")
    ZonedDateTime earliestTime;

    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @Schema(description = "Latest value in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    @XmlElement(name="latest-time")
    ZonedDateTime latestTime;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private TimeSeriesExtents() {}

    public TimeSeriesExtents(final ZonedDateTime versionTime, final ZonedDateTime earliestTime, final ZonedDateTime latestTime ){
        this.versionTime = versionTime;
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
    }

    public TimeSeriesExtents(final Timestamp versionTime, final Timestamp earliestTime, final Timestamp latestTime ) {
        this(toZdt(versionTime),toZdt(earliestTime),toZdt(latestTime));
    }

    private static ZonedDateTime toZdt(final Timestamp time ){
        if( time != null ) {
            return ZonedDateTime.ofInstant(time.toInstant(),ZoneId.of("UTC"));
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


}
