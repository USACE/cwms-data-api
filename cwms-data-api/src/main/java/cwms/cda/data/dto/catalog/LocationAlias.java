package cwms.cda.data.dto.catalog;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

@JsonRootName("alias")
public class LocationAlias {
    @JacksonXmlProperty(isAttribute = true)
    private String name;
    private String value;

    @SuppressWarnings("unused") // for JAXB
    private LocationAlias() {
    }

    public LocationAlias(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "{" + " name='" + getName() + "'" + ", value='" + getValue() + "'" + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocationAlias that = (LocationAlias) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
            return false;

        }
        return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
        return result;
    }
}
