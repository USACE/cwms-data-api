package cwms.cda.data.dto.texttimeseries;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;


@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries extends CwmsDTO {

    private final String name;
    private final Long intervalOffset;
    private final String timeZone;

    private final NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regularMap;

    private final NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap;

    private final Map<String, StandardCatalog> stdCatalog;

    private TextTimeSeries(Builder builder) {
        super(builder.officeId);
        name = builder.name;
        intervalOffset = builder.intervalOffset;
        timeZone = builder.timeZone;

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

        stdCatalog = builder.stdCatalog;
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

    @Nullable
    public Collection<StandardTextTimeSeriesRow> getStandardTextValues() {
        if (stdMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(stdMap.values());
        }
    }

    @Nullable
    public Collection<StandardCatalog> getStandardTextCatalog() {
        if (stdCatalog == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(stdCatalog.values());
        }
    }



    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String name;

        private String officeId;

        private Long intervalOffset;

        private String timeZone;
        NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regMap = null;
        NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap = null;

        private boolean useStdCatalog = true;
        private Map<String, StandardCatalog> stdCatalog;

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

        public Builder withStandardTextValues(Collection<StandardTextTimeSeriesRow> rows) {
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


        public Builder withStandardTextCatalog(Collection<StandardCatalog> catalog ) {
            this.stdCatalog = new LinkedHashMap<>();
            for(StandardCatalog cat : catalog){
                this.stdCatalog.put(cat.getOfficeId(), cat);
            }

            this.useStdCatalog = true;
            return this;
        }

        public Builder useStandardTextCatalog(boolean useStdCatalog) {
            this.useStdCatalog = useStdCatalog;

            if(!useStdCatalog) {
                stdCatalog = null;
            }

            return this;
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

        private static Map<String, StandardCatalog> buildStdCatalog(Map<DateDateKey, StandardTextTimeSeriesRow> rows) {
            Map<String, StandardCatalog> retval = null;

            if (rows != null && !rows.isEmpty()) {
                retval = new LinkedHashMap<>();
                for (StandardTextTimeSeriesRow row : rows.values()) {
                    String rowOfficeId = row.getOfficeId();
                    StandardCatalog catalogForOffice = retval.computeIfAbsent(rowOfficeId,
                            k -> new StandardCatalog(rowOfficeId));

                    catalogForOffice.addValue(row.getStandardTextId(), row.getTextValue());
                }
            }

            return retval;
        }

        private static NavigableMap<DateDateKey, StandardTextTimeSeriesRow> buildStdMap(Map<DateDateKey, StandardTextTimeSeriesRow> rows, boolean useStdCatalog) {
            NavigableMap<DateDateKey, StandardTextTimeSeriesRow> retval = null;

            if (!useStdCatalog) {
                if (rows != null) {
                    retval = new TreeMap<>(rows);
                }
            } else {
                if (rows != null) {
                    retval = new TreeMap<>();
                    Set<Map.Entry<DateDateKey, StandardTextTimeSeriesRow>> entries = rows.entrySet();
                    for (Map.Entry<DateDateKey, StandardTextTimeSeriesRow> entry : entries) {
                        DateDateKey key = entry.getKey();
                        StandardTextTimeSeriesRow row = entry.getValue();

                        StandardTextTimeSeriesRow row2 = new StandardTextTimeSeriesRow.Builder()
                                .from(row)
                                .withTextValue(null)
                                .build();

                        retval.put(key, row2);
                    }
                }
            }

            return retval;
        }

        public TextTimeSeries build() {

            if (useStdCatalog) {
               stdCatalog = buildStdCatalog(stdMap);
            } else {
               stdCatalog = null;
            }

            stdMap = buildStdMap(stdMap, useStdCatalog);

            return new TextTimeSeries(this);
        }
    }
}
