package cwms.cda.data.dto.catalog;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import cwms.cda.data.dto.TimeSeriesExtents;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeseriesCatalogEntry extends CatalogEntry {
    @JacksonXmlProperty(isAttribute = true)
    private String name;

    private String units;
    private String interval;

    @Schema(description = "Offset from top of interval")
    private Long intervalOffset;

    private String timeZone;

    @JacksonXmlElementWrapper(localName = "extents")
    @JacksonXmlProperty(localName = "extents")
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

    @Schema(description = "Only on 21.1.1 Database. The timezone the Interval Offset is from.")
    public String getTimeZone() {
        return timeZone;
    }

    public List<TimeSeriesExtents> getExtents() {
        return extents;
    }

    private TimeseriesCatalogEntry() {
        super(null);
    }

    private TimeseriesCatalogEntry(String office, String name, String units, String interval, Long intervalOffset, String timeZone, List<TimeSeriesExtents> extents) {
        super(office);
        this.name = name;
        this.units = units;
        this.interval = interval;
        this.intervalOffset = intervalOffset;
        this.timeZone = timeZone;
        this.extents = extents;
    }

    public String getUnits() {
        return units;
    }

    @Override
    public String toString() {
        return getOffice() + "/" + name;
    }

    @Override
    public String getCursor() {
        return (getOffice() + "/" + name).toUpperCase();
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
        private List<TimeSeriesExtents> extents = null;

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

        public Builder intervalOffset(final BigDecimal intervalOffset) {
            return intervalOffset(intervalOffset.longValue());
        }

        public Builder timeZone(final String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder withExtent(final TimeSeriesExtents extent) {
            if (extents == null) {
                extents = new ArrayList<>();
            }
            this.extents.add(extent);
            return this;
        }

        public Builder withExtents(final List<TimeSeriesExtents> newExtents) {
            if (newExtents == null) {
                extents = null;
            } else {
                extents = new ArrayList<>();
                extents.addAll(newExtents);
            }
            return this;
        }

        public TimeseriesCatalogEntry build() {
            return new TimeseriesCatalogEntry(office, tsName, units, interval, intervalOffset, timeZone, extents);
        }
    }
}
