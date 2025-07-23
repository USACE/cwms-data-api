package cwms.cda.data.dto.catalog;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
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

    @JacksonXmlElementWrapper(localName = "aliases")
    @JacksonXmlProperty(localName = "alias")
    private Collection<TimeSeriesAlias> aliases;

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

    public Collection<TimeSeriesAlias> getAliases() {
        return aliases;
    }

    private TimeseriesCatalogEntry() {
        super(null);
    }

    private TimeseriesCatalogEntry(Builder builder) {
        super(builder.office);
        this.name = builder.tsName;
        this.units = builder. units;
        this.interval = builder.interval;
        this.intervalOffset = builder.intervalOffset;
        this.timeZone = builder.timeZone;
        this.extents = builder.extents;
        this.aliases = builder.aliases;
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
        private Collection<TimeSeriesAlias> aliases = null;

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

        public Builder withAliases(final Collection<TimeSeriesAlias> aliases) {
            if (aliases == null) {
                this.aliases = null;
            } else {
                this.aliases = new ArrayList<>(aliases);
            }
            return this;
        }

        public TimeseriesCatalogEntry build() {
            return new TimeseriesCatalogEntry(this);
        }
    }
}
