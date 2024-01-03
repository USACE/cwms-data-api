package cwms.cda.data.dto.timeseriestext;

public class StandardCatalogEntry {
    private final String id;
    private final String value;

    public StandardCatalogEntry(String id, String value) {
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
