package cwms.cda.data.dto.texttimeseries;

import static cwms.cda.data.dto.TimeSeries.ZONED_DATE_TIME_FORMAT;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.binarytimeseries.DateDateComparator;
import cwms.cda.formatters.xml.adapters.ZonedDateTimeAdapter;
import hec.data.timeSeriesText.DateDateKey;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.jetbrains.annotations.Nullable;


@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries extends CwmsDTO {


    private final String name;
    private final Long intervalOffset;
    private final String timeZone;

    @Schema(description = "The version type for the text time series being queried. Can be in the form of MAX_AGGREGATE, SINGLE_VERSION, or UNVERSIONED. " +
            "MAX_AGGREGATE will get the latest version date value for each value in the date range. SINGLE_VERSION must be called with a valid " +
            "version date and will return the values for the version date provided. UNVERSIONED return values from an unversioned time series. " +
            "Note that SINGLE_VERSION requires a valid version date while MAX_AGGREGATE and UNVERSIONED each require a null version date.")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    VersionType dateVersionType;

    @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = ZONED_DATE_TIME_FORMAT)
    @Schema(description = "The version date of the time series trace")
    ZonedDateTime versionDate;

    private final NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regularMap;


    private TextTimeSeries(Builder builder) {
        super(builder.officeId);
        name = builder.name;
        intervalOffset = builder.intervalOffset;
        timeZone = builder.timeZone;
        dateVersionType = builder.dateVersionType;
        versionDate = builder.versionDate;

        if (builder.regMap == null) {
            regularMap = null;
        } else {
            regularMap = new TreeMap<>(builder.regMap);
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

    public ZonedDateTime getVersionDate() {
        return versionDate;
    }

    @Override
    public void validate() throws FieldException {

    }

    @JsonIgnore
    @Nullable
    public NavigableMap<DateDateKey, RegularTextTimeSeriesRow> getTextTimeSeriesMap() {
        if (regularMap == null) {
            return null;
        }
        return Collections.unmodifiableNavigableMap(regularMap);
    }

    @Nullable
    public Collection<RegularTextTimeSeriesRow> getRegularTextValues() {
        if (regularMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(regularMap.values());
        }
    }





    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String name;

        private String officeId;

        private Long intervalOffset;

        private String timeZone;

        public VersionType dateVersionType;
        public ZonedDateTime versionDate;
        NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regMap = null;





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

        public Builder withVersionDate(ZonedDateTime versionDate) {
            this.versionDate = versionDate;
            return this;
        }




        public Builder withRows(Collection<TextTimeSeriesRow> rows) {
            if (rows == null) {
                regMap = null;

            } else {
                if (regMap == null) {
                    regMap = new TreeMap<>(new DateDateComparator());
                } else {
                    regMap.clear();
                }


                for (TextTimeSeriesRow row : rows) {
                    withRow(row);
                }
            }
            return this;
        }

        public Builder withRow(TextTimeSeriesRow row) {
            Builder retval = this;
            if (row instanceof RegularTextTimeSeriesRow) {
                retval = withRegRow((RegularTextTimeSeriesRow) row);
            }
            return retval;
        }


        public Builder withRegularTextValues(List<RegularTextTimeSeriesRow> rows) {
            if (rows == null) {
                regMap = null;
            } else {
                if (regMap == null) {
                    regMap = new TreeMap<>(new DateDateComparator());
                } else {
                    regMap.clear();
                }

                for (RegularTextTimeSeriesRow row : rows) {
                    if (row != null) {
                        regMap.put(row.getDateDateKey(), row);
                    }
                }
            }
            return this;
        }

        public Builder withRegRow(RegularTextTimeSeriesRow row) {
            if (row != null) {
                if (regMap == null) {
                    regMap = new TreeMap<>(new DateDateComparator());
                }
                regMap.put(row.getDateDateKey(), row);
            }
            return this;
        }

        public TextTimeSeries build() {
            return new TextTimeSeries(this);
        }
    }
}
