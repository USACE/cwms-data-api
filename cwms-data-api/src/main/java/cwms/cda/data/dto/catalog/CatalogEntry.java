package cwms.cda.data.dto.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public abstract class CatalogEntry {
    @JacksonXmlProperty(isAttribute = true)
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
