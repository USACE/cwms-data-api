package cwms.cda.data.dto.timeseriestext;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class StandardCatalog extends CwmsDTO {

    private final Map<String, StandardCatalogEntry> idMap = new LinkedHashMap<>();


    public StandardCatalog(String officeId) {
        super(officeId);
    }


    public Collection<StandardCatalogEntry> getEntries() {
        return Collections.unmodifiableCollection(idMap.values());
    }

    public void addValue(String id, String value) {
        idMap.put(id, new StandardCatalogEntry(id, value));
    }

    @Override
    public void validate() throws FieldException {

    }

}
