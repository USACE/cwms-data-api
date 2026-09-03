package cwms.cda.data.dto.rating;

import cwms.cda.data.dto.CwmsDTOBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;

@JsonDeserialize(using = IndependentRoundingSpecDeserializer.class)
public class IndependentRoundingSpec extends CwmsDTOBase {
    @JacksonXmlProperty(isAttribute = true)
    private final Integer position;

    @JacksonXmlText
    private final String value;

    public IndependentRoundingSpec(@JsonProperty("position") Integer position, @JsonProperty(
            "value") String value) {
        this.position = position;
        this.value = value;
    }

    public IndependentRoundingSpec(String value) {
        this.position = null;
        this.value = value;
    }

    public Integer getPosition() {
        return position;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final IndependentRoundingSpec that = (IndependentRoundingSpec) o;

        return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
    }

    @Override
    public int hashCode() {
        return getValue() != null ? getValue().hashCode() : 0;
    }
}
