package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StandardCatalogEntry {
    private final String id;
    private final String value;

    public StandardCatalogEntry(@JsonProperty("id")String id, @JsonProperty("value")String value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getValue() {
        return value;
    }
}
