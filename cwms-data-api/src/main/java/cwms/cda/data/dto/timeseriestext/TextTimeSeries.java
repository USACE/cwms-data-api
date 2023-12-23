package cwms.cda.data.dto.timeseriestext;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import hec.data.timeSeriesText.DateDateComparator;
import hec.data.timeSeriesText.DateDateKey;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * This is a DTO version of hec.data.timeSeriesText.TextTimeSeries
 * For now we can try and just use the hec.data TextTimeSeriesRow interface.
 * TODO: Do we want to use ZonedDateTime instead of Date in the rows?
 * Not sure if we really need this class to hold a specific <T> type.
 * TODO: Can the row types be intermixed?  Mike says yes
 */


@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries extends CwmsDTO {
//  This Monolith version of this class has:
// https://bitbucket.hecdev.net/projects/SC/repos/hec-monolith/browse/hec-monolith/src/main/java/hec/data/timeSeriesText/TextTimeSeries.java
//    private ITimeSeriesDescription _timeSeriesDescription;

    // Instead of ITimeSeriesDescription lets just have tsid
    private String id;

    private NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regularMap;
    private NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap;

    private TextTimeSeries(Builder builder) {
        super(builder.officeId);
        id = builder.id;
        if (builder.regMap == null) {
            regularMap = null;
        } else {
            regularMap = new TreeMap<>(builder.regMap);
        }

        if (builder.stdMap == null) {
            stdMap = null;
        } else {
            stdMap = new TreeMap<>(builder.stdMap);
        }
    }

    public String getId() {
        return id;
    }

    @Override
    public void validate() throws FieldException {

    }

    @JsonIgnore
    public NavigableMap<DateDateKey, RegularTextTimeSeriesRow> getTextTimeSeriesMap() {
        return Collections.unmodifiableNavigableMap(regularMap);
    }

    public Collection<RegularTextTimeSeriesRow> getRegRows() {
        if (regularMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(regularMap.values());
        }
    }

    public Collection<StandardTextTimeSeriesRow> getStdRows() {
        if (stdMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(stdMap.values());
        }
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String id;

        private String officeId;
        NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regMap = null;
        NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap = null;

        public Builder() {
        }

        public Builder withId(String tsid) {
            this.id = tsid;
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }


        public Builder withRows(Collection<TextTimeSeriesRow> rows) {
            if (rows == null) {
                regMap = null;
                stdMap = null;
            } else {
                if (regMap == null) {
                    regMap = new TreeMap<>(new DateDateComparator());
                } else {
                    regMap.clear();
                }

                if (stdMap == null) {
                    stdMap = new TreeMap<>(new DateDateComparator());
                } else {
                    stdMap.clear();
                }

                for (TextTimeSeriesRow row : rows) {
                    withRow(row);
                }
            }
            return this;
        }

        public Builder withRow(TextTimeSeriesRow row) {
            Builder retval = this;
            if (row instanceof StandardTextTimeSeriesRow) {
                retval = withStdRow((StandardTextTimeSeriesRow) row);
            } else if (row instanceof RegularTextTimeSeriesRow) {
                retval =  withRegRow((RegularTextTimeSeriesRow) row);
            }

            return retval;
        }
        public Builder withStdRows(Collection<StandardTextTimeSeriesRow> rows) {
            if (rows == null) {
                stdMap = null;
            } else {
                if (stdMap == null) {
                    stdMap = new TreeMap<>(new DateDateComparator());
                } else {
                    stdMap.clear();
                }

                for (StandardTextTimeSeriesRow row : rows) {
                    if (row != null) {
                        stdMap.put(row.getDateDateKey(), row);
                    }
                }
            }
            return this;
        }


        public Builder withStdRow(StandardTextTimeSeriesRow row) {
            if (row != null) {
                if (stdMap == null) {
                    stdMap = new TreeMap<>(new DateDateComparator());
                }
                stdMap.put(row.getDateDateKey(), row);
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
