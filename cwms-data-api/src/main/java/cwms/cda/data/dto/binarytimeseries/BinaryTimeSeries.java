package cwms.cda.data.dto.binarytimeseries;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import hec.data.timeSeriesText.DateDateKey;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;

@JsonDeserialize(builder = BinaryTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class BinaryTimeSeries extends CwmsDTO {
    private final String name;
    private final Long intervalOffset;
    private final String timeZone;

    @Schema(description = "The version type for the binary time series being queried. Can be in the form of MAX_AGGREGATE, SINGLE_VERSION, or UNVERSIONED. "
            + "MAX_AGGREGATE will get the latest version date value for each value in the date range. SINGLE_VERSION must be called with a valid "
            + "version date and will return the values for the version date provided. UNVERSIONED return values from an unversioned time series. "
            + "Note that SINGLE_VERSION requires a valid version date while MAX_AGGREGATE and UNVERSIONED each require a null version date.")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    VersionType dateVersionType;

    @Schema(description = "The version date of the time series trace")
    Instant versionDate;

    private final NavigableMap<DateDateKey, BinaryTimeSeriesRow> entries;

    private BinaryTimeSeries(Builder builder) {
        super(builder.officeId);
        name = builder.name;
        intervalOffset = builder.intervalOffset;
        timeZone = builder.timeZone;

        dateVersionType = builder.dateVersionType;
        versionDate = builder.versionDate;

        if (builder.entriesMap != null) {
            entries = new TreeMap<>(builder.entriesMap);
        } else {
            entries = null;
        }

    }

    public String getName() {
        return name;
    }

    public Long getIntervalOffset() {
        return intervalOffset;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public VersionType getDateVersionType() {
        return dateVersionType;
    }

    public Instant getVersionDate() {
        return versionDate;
    }

    @Nullable
    public Collection<BinaryTimeSeriesRow> getBinaryValues() {
        if (entries == null) {
            return null;
        }
        return Collections.unmodifiableCollection(entries.values());
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String name;

        private String officeId;

        private Long intervalOffset;
        private String timeZone;
        private VersionType dateVersionType;
        private Instant versionDate;

        NavigableMap<DateDateKey, BinaryTimeSeriesRow> entriesMap = null;

        public Builder() {
        }

        public Builder withName(String tsid) {
            this.name = tsid;
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withIntervalOffset(Long intervalOffset) {
            this.intervalOffset = intervalOffset;
            return this;
        }

        public Builder withTimeZone(String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder withDateVersionType(VersionType dateVersionType) {
            this.dateVersionType = dateVersionType;
            return this;
        }

        public Builder withVersionDate(Instant versionDate) {
            this.versionDate = versionDate;
            return this;
        }


        public Builder withBinaryValues(Collection<BinaryTimeSeriesRow> rows) {
            if (rows == null) {
                entriesMap = null;

            } else {
                if (entriesMap == null) {
                    entriesMap = new TreeMap<>(new DateDateComparator());
                } else {
                    entriesMap.clear();
                }

                for (BinaryTimeSeriesRow row : rows) {
                    withBinaryValue(row);
                }
            }
            return this;
        }

        public Builder withBinaryValue(BinaryTimeSeriesRow row) {
            if (row != null) {
                if (entriesMap == null) {
                    entriesMap = new TreeMap<>(new DateDateComparator());
                }

                entriesMap.put(buildDateDateKey(row.getDateTime(), row.getDataEntryDate()), row);
            }
            return this;
        }

        private DateDateKey buildDateDateKey(Instant dateTime, Instant dataEntry) {
            Date dateTimeDate = dateTime == null ? null : Date.from(dateTime);
            Date dataEntryDate = dataEntry == null ? null : Date.from(dataEntry);
            return new DateDateKey(dateTimeDate, dataEntryDate);
        }

        public BinaryTimeSeries build() {

            return new BinaryTimeSeries(this);
        }
    }

}
