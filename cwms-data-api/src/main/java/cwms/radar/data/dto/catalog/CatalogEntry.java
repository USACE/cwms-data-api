package cwms.radar.data.dto.catalog;

import javax.xml.bind.annotation.*;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({TimeseriesCatalogEntry.class,LocationCatalogEntry.class})
public abstract class CatalogEntry {
    @XmlAttribute
    private String office;

    @SuppressWarnings("unused") // for JAXB rendering
    private CatalogEntry(){}

    public CatalogEntry(String office){
        this.office = office;
    }

    public String getOffice(){
        return office;
    }

    public abstract String getCursor();
}
