package cwms.cda.data.dto.timeseriestext;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import org.jetbrains.annotations.NotNull;

@JsonDeserialize(builder = StandardTextValue.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties("office-id")  // This does work to block office-id from the super!
public class StandardTextValue extends CwmsDTO {

    private final StandardTextId id;

    private final String standardText;

    private StandardTextValue(@NotNull Builder builder) {
        super(builder.id == null? null : builder.id.getOfficeId());
        this.id = builder.id;
        this.standardText = builder.standardText;
    }

    public String getStandardText() {
        return standardText;
    }


    public StandardTextId getId() {
        return id;
    }

    @Override
    public void validate() throws FieldException {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StandardTextValue that = (StandardTextValue) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        return getStandardText() != null ? getStandardText().equals(that.getStandardText()) : that.getStandardText() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getStandardText() != null ? getStandardText().hashCode() : 0);
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private StandardTextId id;
        private String standardText;

        public Builder() {

        }

        public Builder withStandardText(String text) {
            standardText = text;
            return this;
        }

        @JsonProperty("id")  // If we don't have this then Jackson gets confused whether to call this or the method that takes hec.data
        public Builder withId(StandardTextId standardTextId) {
            if (standardTextId != null) {
                this.id =  new StandardTextId.Builder().from(standardTextId).build();
            } else {
                this.id = null;
            }
            return this;
        }

        public Builder withId(hec.data.timeSeriesText.StandardTextId newId) {
            return withId(new StandardTextId.Builder().from(newId).build());
        }

        public Builder from(StandardTextValue input) {
            StandardTextId newId = null;
            String newStandardText = null;

            if (input != null) {
                newId = input.getId();
                newStandardText = input.getStandardText();
            }
            return withId(newId)
                    .withStandardText(newStandardText);
        }

        // Simplify building from hec.data object.
        public Builder from(hec.data.timeSeriesText.StandardTextValue value) {
            StandardTextId newId = null;
            String newStandardText = null;

            if (value != null) {
                hec.data.timeSeriesText.StandardTextId standardTextId = value.getStandardTextId();
                newId = new StandardTextId.Builder().from(standardTextId).build();
                newStandardText = value.getStandardText();
            }

            return withId(newId)
                    .withStandardText(newStandardText);
        }


        public StandardTextValue build() {
            return new StandardTextValue(this);
        }
    }
}
