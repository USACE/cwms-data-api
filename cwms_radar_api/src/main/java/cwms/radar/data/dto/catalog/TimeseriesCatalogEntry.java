package cwms.radar.data.dto.catalog;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.fasterxml.jackson.annotation.JsonFormat;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import io.swagger.v3.oas.annotations.media.Schema;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeseriesCatalogEntry extends CatalogEntry{
    @XmlAttribute(name="ts-name")
    private String tsName;
    private String longName;
    private String units;
    private String interval;

    @Schema(description="Offset from top of interval")
    @XmlElement(name="interval-offset")
    private Long intervalOffset;

    @XmlElement(name="time-zone")
    private String timeZone;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @XmlElement(name="earliest-time")
    private ZonedDateTime earliestTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @XmlElement(name="latest-time")
    private ZonedDateTime latestTime;


    public String getTsName() {
        return this.tsName;
    }

    public String getLongName() {
        return this.longName;
    }

    public String getFullName(){
        return tsName;
    }

    public String getInterval() {
        return interval;
    }

    public Long getIntervalOffset() {
        return intervalOffset;
    }

    @Schema( description = "Only on 21.1.1 Database. The timezone the Interval Offset is from.")
    public String getTimeZone() {
        return timeZone;
    }

    public ZonedDateTime getEarliestTime()
    {
        return earliestTime;
    }

    public ZonedDateTime getLatestTime()
    {
        return latestTime;
    }

    private TimeseriesCatalogEntry(){ super(null);}

    private TimeseriesCatalogEntry(String office, String name, String units, String interval, Long intervalOffset, String timeZone, ZonedDateTime earliestTime, ZonedDateTime latestTime){
        super(office);
        this.tsName=name;
        this.units = units;
        this.interval = interval;
        this.intervalOffset = intervalOffset;
        this.timeZone = timeZone;
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
    }

    public String getUnits(){
        return units;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(getOffice()).append("/").append(tsName).append(";units=").append(units);
        return builder.toString();
    }

    public static class Builder {
        private String office;
        private String tsName;
        private String longName;
        private String units;
        private String interval;
        private Long intervalOffset;
        private String timeZone = null;
        private ZonedDateTime earliestTime;
        private ZonedDateTime latestTime;

        public Builder officeId(final String office) {
            this.office = office;
            return this;
        }

        public Builder cwmsTsId(final String tsId) {
            this.tsName = tsId;
            return this;
        }

        public Builder units(final String units) {
            this.units = units;
            return this;
        }

        public Builder interval(final String interval) {
            this.interval = interval;
            return this;
        }

        public Builder intervalOffset(final Long intervalOffset) {
            this.intervalOffset = intervalOffset;
            return this;
        }

        public Builder intervalOffset( final BigDecimal intervalOffset ) {
            return intervalOffset(intervalOffset.longValue());
        }

        public Builder timeZone(final String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder earliestTime(final Timestamp earliest) {
            ZonedDateTime zdt = null;
            if(earliest != null)
            {
                zdt = ZonedDateTime.ofInstant(earliest.toInstant(), ZoneId.of("UTC"));
            }
            return earliestTime(zdt);
        }

        public Builder earliestTime(final ZonedDateTime earliest) {
            this.earliestTime = earliest;
            return this;
        }

        public Builder latestTime(final Timestamp latest) {
            ZonedDateTime zdt = null;
            if(latest != null)
            {
                zdt = ZonedDateTime.ofInstant(latest.toInstant(), ZoneId.of("UTC"));
            }
            return latestTime(zdt);
        }

        public Builder latestTime(final ZonedDateTime latest) {
            this.latestTime = latest;
            return this;
        }

        public TimeseriesCatalogEntry build(){
            return new TimeseriesCatalogEntry(office, tsName, units, interval, intervalOffset, timeZone, earliestTime, latestTime);
        }
    }
}
