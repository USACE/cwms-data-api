package cwms.radar.data.dto.catalog;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClobCatalogEntry extends CatalogEntry{
    @XmlAttribute
    public String id;
    public String description;

    public String getId() {
        return this.id;
    }

    public String getDescription() {
        return this.description;
    }

    @SuppressWarnings("unused") // for jaxb
    private ClobCatalogEntry(){super(null);}


    public ClobCatalogEntry(String office) {
        super(office);

    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(getOffice()).append("/").append(id).append(";description=").append(description);
        return builder.toString();
    }
}
