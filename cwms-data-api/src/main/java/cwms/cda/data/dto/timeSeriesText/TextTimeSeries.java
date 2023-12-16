package cwms.cda.data.dto.timeSeriesText;

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
 * @param <T>
 */


@JsonDeserialize(builder = TextTimeSeries.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TextTimeSeries<T extends TextTimeSeriesRow> extends CwmsDTO {
//  This Monolith version of this class has:
// https://bitbucket.hecdev.net/projects/SC/repos/hec-monolith/browse/hec-monolith/src/main/java/hec/data/timeSeriesText/TextTimeSeries.java
//    private ITimeSeriesDescription _timeSeriesDescription;

    // Instead of ITimeSeriesDescription lets just have tsid
    private String id;

    private NavigableMap<DateDateKey, T> textTimeSeriesMap;

    private TextTimeSeries(Builder<T> builder) {
        super(builder == null ? null : builder.officeId);
        id = builder.id;
        textTimeSeriesMap = new TreeMap<>(builder.textTimeSeriesMap);
    }

    public String getId() {
        return id;
    }

    @Override
    public void validate() throws FieldException {

    }

    @JsonIgnore
    public NavigableMap<DateDateKey, T> getTextTimeSeriesMap() {
        return Collections.unmodifiableNavigableMap(textTimeSeriesMap);
    }

    public Collection<T> getRows(){
        return Collections.unmodifiableCollection(textTimeSeriesMap.values());
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder<B extends TextTimeSeriesRow> {
        private String id;

        private String officeId;
        NavigableMap<DateDateKey, B> textTimeSeriesMap = new TreeMap<>(new DateDateComparator());

        public Builder() {
        }

        public Builder<B> withId(String tsid){
            this.id = tsid;
            return this;
        }

        public Builder<B> withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }


        public Builder<B> withRows(Collection<B> rows) {
            this.textTimeSeriesMap.clear();
            if (textTimeSeriesMap != null) {

                for (B row : rows) {
                    if(row != null) {
                        textTimeSeriesMap.put(row.getDateDateKey(), row);
                    }
                }
            }
            return this;
        }

        public Builder<B> withRow(B row) {
            if(row != null) {
                textTimeSeriesMap.put(row.getDateDateKey(), row);
            }
            return this;
        }


        public TextTimeSeries<B> build(){
            return new TextTimeSeries<>(this);
        }
    }
}
