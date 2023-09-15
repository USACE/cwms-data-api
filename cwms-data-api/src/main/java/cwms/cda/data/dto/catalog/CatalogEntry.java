package cwms.cda.data.dto.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

@XmlRootElement(name = "entry")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({TimeseriesCatalogEntry.class,LocationCatalogEntry.class})
public abstract class CatalogEntry {
    @XmlAttribute
    private String office;

    @SuppressWarnings("unused") // for JAXB rendering
    private CatalogEntry(){

    }

    protected CatalogEntry(String office) {
        this.office = office;
    }

    public String getOffice() {
        return office;
    }

    @JsonIgnore
    public abstract String getCursor();
}
