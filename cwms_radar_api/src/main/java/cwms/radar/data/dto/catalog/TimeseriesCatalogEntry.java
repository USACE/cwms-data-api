package cwms.radar.data.dto.catalog;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.fasterxml.jackson.annotation.JsonFormat;

import cwms.radar.data.dto.TimeSeriesExtents;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import io.swagger.v3.oas.annotations.media.Schema;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeseriesCatalogEntry extends CatalogEntry{
    @XmlAttribute
    private String name;

    private String units;
    private String interval;

    @Schema(description="Offset from top of interval")
    @XmlElement(name="interval-offset")
    private Long intervalOffset;

    @XmlElement(name="time-zone")
    private String timeZone;

    @XmlElementWrapper(name="extents")
    @XmlElement(name="extents")
    private List<TimeSeriesExtents> extents;


    public String getName() {
        return this.name;
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

    public List<TimeSeriesExtents> getExtents()
    {
        return extents;
    }



    private TimeseriesCatalogEntry(){ super(null);}

    private TimeseriesCatalogEntry(String office, String name, String units, String interval, Long intervalOffset, String timeZone, List<TimeSeriesExtents> extents){
        super(office);
        this.name=name;
        this.units = units;
        this.interval = interval;
        this.intervalOffset = intervalOffset;
        this.timeZone = timeZone;
        this.extents = extents;
    }

    public String getUnits(){
        return units;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(getOffice()).append("/").append(name);
        return builder.toString();
    }

    public static class Builder {
        private String office;
        private String tsName;
        private String units;
        private String interval;
        private Long intervalOffset;
        private String timeZone = null;
        private ZonedDateTime earliestTime;
        private ZonedDateTime latestTime;
        private ArrayList<TimeSeriesExtents> extents = new ArrayList<>();

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

        public Builder withExtent(final TimeSeriesExtents extent ) {
            this.extents.add(extent);
            return this;
        }



        public TimeseriesCatalogEntry build(){
            return new TimeseriesCatalogEntry(office, tsName, units, interval, intervalOffset, timeZone, extents);
        }
    }
}
