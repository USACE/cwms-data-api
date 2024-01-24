package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class StandardCatalog extends CwmsDTO {

    private final Map<String, StandardCatalogEntry> idMap = new LinkedHashMap<>();


    public StandardCatalog(@JsonProperty("office-id")String officeId) {
        super(officeId);
    }


    public Collection<StandardCatalogEntry> getEntries() {
        return Collections.unmodifiableCollection(idMap.values());
    }

    public void setEntries(Collection<StandardCatalogEntry> entries) {
        idMap.clear();
        for(StandardCatalogEntry entry : entries){
            idMap.put(entry.getId(), entry);
        }
    }

    public void addValue(String id, String value) {
        idMap.put(id, new StandardCatalogEntry(id, value));
    }

    @Override
    public void validate() throws FieldException {

    }

}
