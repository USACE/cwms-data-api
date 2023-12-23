package cwms.cda.data.dto.timeseriestext;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;

/**
 * Almost an exact copy of hec.data.timeSeriesText.StandardTextId.  Its duplicated here so that
 * CDA can
 * control serialization.
 */

@JsonDeserialize(builder = StandardTextId.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class StandardTextId extends CwmsDTO {

    private final String id;

    private StandardTextId(Builder builder) {
        super(builder.officeId);

        this.id = builder.id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StandardTextId that = (StandardTextId) o;

        return getId() != null ? getId().equals(that.getId()) : that.getId() == null;
    }

    @Override
    public int hashCode() {
        return getId() != null ? getId().hashCode() : 0;
    }

    @Override
    public void validate() throws FieldException {

    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        public String officeId;
        private String id;

        public Builder() {
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder from(StandardTextId from) {
            String newOfficeId = null;
            String newStandardTextId = null;

            if (from != null) {
                newOfficeId = from.getOfficeId();
                newStandardTextId = from.getId();
            }
            return withOfficeId(newOfficeId)
                    .withId(newStandardTextId);
        }

        // simplify building from a hec.data object.
        public Builder from(hec.data.timeSeriesText.StandardTextId from) {
            String newOfficeId = null;
            String newStandardTextId = null;

            if (from != null) {
                newOfficeId = from.getOfficeId();
                newStandardTextId = from.getStandardTextId();
            }
            return withOfficeId(newOfficeId)
                    .withId(newStandardTextId);
        }


        public StandardTextId build() {
            return new StandardTextId(this);
        }

    }
}
