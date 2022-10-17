package cwms.radar.data.dto.catalog;

import javax.xml.bind.annotation.*;

@XmlRootElement(name="alias")
@XmlAccessorType(XmlAccessType.FIELD)
public class LocationAlias {
    @XmlAttribute
    private String name;
    @XmlValue
    private String value;

    @SuppressWarnings("unused") // for JAXB
    private LocationAlias(){}

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
        return "{" +
            " name='" + getName() + "'" +
            ", value='" + getValue() + "'" +
            "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocationAlias that = (LocationAlias) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null)
            return false;
        return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
        return result;
    }
}
