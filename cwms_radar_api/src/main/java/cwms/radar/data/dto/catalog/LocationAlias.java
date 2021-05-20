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
    
    
}
