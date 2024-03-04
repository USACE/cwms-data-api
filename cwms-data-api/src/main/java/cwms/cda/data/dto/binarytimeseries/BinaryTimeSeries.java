package cwms.cda.data.dto.binarytimeseries;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import hec.data.timeSeriesText.DateDateKey;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;

@JsonDeserialize(builder = BinaryTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class BinaryTimeSeries extends CwmsDTO {
    private final String name;
    private final Long intervalOffset;
    private final String timeZone;

    private final NavigableMap<DateDateKey, BinaryTimeSeriesRow> entries;

    private BinaryTimeSeries(Builder builder) {
        super(builder.officeId);
        name = builder.name;
        intervalOffset = builder.intervalOffset;
        timeZone = builder.timeZone;

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

    @Nullable
    public Collection<BinaryTimeSeriesRow> getBinaryValues() {
        if (entries == null) {
            return null;
        }
        return Collections.unmodifiableCollection(entries.values());
    }

    @Override
    public void validate() throws FieldException {

    }


    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String name;

        private String officeId;

        private Long intervalOffset;
        private String timeZone;

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
                entriesMap.put(new DateDateKey(row.getDateTime()==null?null:Date.from(row.getDateTime()), row.getDataEntryDate()==null?null:Date.from(row.getDataEntryDate())), row);
            }
            return this;
        }

        public BinaryTimeSeries build() {

            return new BinaryTimeSeries(this);
        }
    }

}
