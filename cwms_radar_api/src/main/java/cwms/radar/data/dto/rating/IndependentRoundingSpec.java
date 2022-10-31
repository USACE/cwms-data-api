package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IndependentRoundingSpec {
    private final Integer position;

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
