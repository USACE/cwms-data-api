package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import org.jetbrains.annotations.NotNull;

@JsonDeserialize(builder = StandardTextCatalog.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties("office-id")  // This blocks office-id from the super!
public class StandardTextCatalog extends CwmsDTO {

    private final NavigableMap<StandardTextId, StandardTextValue> values;


    private StandardTextCatalog(@NotNull Builder builder) {
        super(getOffice(builder.values));

        if (builder.values != null) {
            values = new java.util.TreeMap<>(new StandardTextIdComparator());
            for (StandardTextValue value : builder.values) {
                values.put(value.getId(), value);
            }
        } else {
            values = null;
        }

    }

    private static String getOffice(Collection<StandardTextValue> values) {
        String retval = null;
        if (values != null) {
            for (StandardTextValue value : values) {
                if (value != null) {
                    StandardTextId id = value.getId();
                    if (id != null && id.getOfficeId() != null) {
                        retval = id.getOfficeId();
                        break;
                    }
                }
            }
        }
        return retval;
    }

    public Collection<StandardTextValue> getValues() {
        if (values == null) {
            return null;
        }
        return values.values();
    }


    @Override
    public void validate() throws FieldException {

    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private List<StandardTextValue> values = new ArrayList<>();


        public Builder() {
        }


        /**
         * Replaces the list of values.
         *
         * @param newValues
         * @return
         */
        public Builder withValues(List<StandardTextValue> newValues) {

            if (newValues != null) {
                values = new ArrayList<>();
                values.addAll(newValues);
            } else {
                values = null;
            }

            return this;
        }

        /**
         * Adds a value to the list of values.
         *
         * @param standardTextValue
         * @return
         */
        public Builder withValue(StandardTextValue standardTextValue) {
            if (values == null) {
                values = new ArrayList<>();
            }
            values.add(standardTextValue);
            return this;
        }

        public StandardTextCatalog build() {
            return new StandardTextCatalog(this);
        }

        public Builder from(hec.data.timeSeriesText.StandardTextCatalog dataCatalog) {
            List<StandardTextValue> newValues = null;

            if (dataCatalog != null) {
                Collection<hec.data.timeSeriesText.StandardTextValue> standardTextValues =
                        dataCatalog.getStandardTextValues();
                if (standardTextValues != null) {
                    newValues = new ArrayList<>();
                    for (hec.data.timeSeriesText.StandardTextValue standardTextValue :
                            standardTextValues) {
                        StandardTextValue newStandardTextValue =
                                new StandardTextValue.Builder().from(standardTextValue).build();
                        withValue(newStandardTextValue);
                    }
                }
            }

            return withValues(newValues);
        }


    }


}
