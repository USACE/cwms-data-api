package cwms.radar.data.dto;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Base64.Encoder;

import javax.xml.bind.annotation.*;

import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.LocationCatalogEntry;
import cwms.radar.data.dto.catalog.TimeseriesCatalogEntry;
import io.swagger.v3.oas.annotations.media.Schema;

@XmlRootElement(name="catalog")
@XmlAccessorType(XmlAccessType.FIELD)
public class Catalog extends CwmsDTOPaginated {
    @Schema(
        oneOf = {
            LocationCatalogEntry.class,
            TimeseriesCatalogEntry.class
        }
    )
    @XmlElementWrapper(name="entries")
    @XmlElement(name="entry")
    private List<? extends CatalogEntry> entries;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private Catalog(){}

    public Catalog(String page, int total, int pageSize, List<? extends CatalogEntry> entries ){
        super(page, pageSize);
        this.total = total;

        Objects.requireNonNull(entries, "List of catalog entries must be a valid list, even if empty");
        this.entries = entries;
        if( entries.size() == pageSize){
            nextPage = encodeCursor(entries.get(entries.size()-1).toString().toUpperCase(), total, "|||");
        } else {
            nextPage = null;
        }
    }

    /**
     * @return List<? extends CatalogEntry> return the entries
     */
    public List<? extends CatalogEntry> getEntries() {
        return entries;
    }

}
