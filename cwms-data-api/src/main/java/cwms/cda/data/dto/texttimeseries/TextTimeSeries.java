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


@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries extends CwmsDTO {
//  This Monolith version of this class has:
// https://bitbucket.hecdev.net/projects/SC/repos/hec-monolith/browse/hec-monolith/src/main/java/hec/data/timeSeriesText/TextTimeSeries.java
//    private ITimeSeriesDescription _timeSeriesDescription;
    
    private final String id;

    private final NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regularMap;
    private final NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap;
    private Map<String, StandardCatalog> stdCatalog = null;

    private TextTimeSeries(Builder builder) {
        super(builder.officeId);
        id = builder.id;
        if (builder.regMap == null) {
            regularMap = null;
        } else {
            regularMap = new TreeMap<>(builder.regMap);
        }

        if (builder.useStdCatalog) {
            if(builder.stdCat == null){
                stdCatalog = buildStdCatalog(builder.stdMap);
            } else {
                stdCatalog = new LinkedHashMap<>();
                for(StandardCatalog cat : builder.stdCat){
                    stdCatalog.put(cat.getOfficeId(), cat);
                }
            }
        }
        stdMap = buildStdMap(builder.stdMap, builder.useStdCatalog);
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

    public Collection<RegularTextTimeSeriesRow> getRegularTextValues() {
        if (regularMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(regularMap.values());
        }
    }

    public Collection<StandardTextTimeSeriesRow> getStandardTextValues() {
        if (stdMap == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(stdMap.values());
        }
    }

    public Collection<StandardCatalog> getStandardTextCatalog() {
        if (stdCatalog == null) {
            return null;
        } else {
            return Collections.unmodifiableCollection(stdCatalog.values());
        }
    }

    public static Map<String, StandardCatalog> buildStdCatalog(Map<DateDateKey, StandardTextTimeSeriesRow> rows) {
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

    public static NavigableMap<DateDateKey, StandardTextTimeSeriesRow> buildStdMap(Map<DateDateKey, StandardTextTimeSeriesRow> rows, boolean useStdCatalog) {
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

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String id;

        private String officeId;
        NavigableMap<DateDateKey, RegularTextTimeSeriesRow> regMap = null;
        NavigableMap<DateDateKey, StandardTextTimeSeriesRow> stdMap = null;

        private boolean useStdCatalog = true;
        private Collection<StandardCatalog> stdCat;

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
            this.stdCat = catalog;
            this.useStdCatalog = true;
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


        public TextTimeSeries build() {
            return new TextTimeSeries(this);
        }


    }
}
