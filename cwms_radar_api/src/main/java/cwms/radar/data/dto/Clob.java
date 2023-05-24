package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import cwms.radar.api.errors.FieldException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "clob")
@XmlAccessorType(XmlAccessType.FIELD)
public class Clob extends CwmsDTO {
    @JsonProperty(required = true)
    private String id;
    private String description;
    private String value;

    @SuppressWarnings("unused")
    private Clob() {
        super(null);
    }

    public Clob(String office, String id, String description, String value) {
        super(office);
        this.id = id;
        this.description = description;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getOfficeId()).append("/").append(id).append(";description=").append(description);
        return builder.toString();
    }

    @Override
    public void validate() throws FieldException {
    }
}
