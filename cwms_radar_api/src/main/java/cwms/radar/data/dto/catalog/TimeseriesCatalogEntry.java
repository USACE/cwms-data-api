package cwms.radar.data.dto.catalog;

import java.math.BigDecimal;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import io.swagger.v3.oas.annotations.media.Schema;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeseriesCatalogEntry extends CatalogEntry{
    @XmlAttribute
    private String tsName;
    private String longName;
    private String units;
    private String interval;
    @Schema(description="Offset from top of interval")
    private Long intervalOffset;
    private String timeZone;

    public String getTsName() {
        return this.tsName;
    }

    public String getLongName() {
        return this.longName;
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

    private TimeseriesCatalogEntry(){ super(null);}

    private TimeseriesCatalogEntry(String office, String name, String units, String interval, Long intervalOffset, String timeZone){
        super(office);
        this.tsName=name;
        this.units = units;
        this.interval = interval;
        this.intervalOffset = intervalOffset;
        this.timeZone = timeZone;
    }

    public String getFullName(){
        return tsName;
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
            this.intervalOffset = intervalOffset.longValue();
            return this;
        }

        public Builder timeZone(final String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public TimeseriesCatalogEntry build(){
            return new TimeseriesCatalogEntry(office, tsName, units, interval, intervalOffset, timeZone);
        }
    }
}
