package cwms.radar.data.dto;

import java.util.List;
import java.util.Objects;

public class Catalog implements CwmsDTO {
    private int page;
    private int nextPage;
    private int total;
    private List<CatalogEntry> entries;

    public Catalog(int page, int nextPage, int total, List<CatalogEntry> entries ){
        this.page = page;
        this.nextPage = nextPage;
        this.total = total;

        Objects.requireNonNull(entries, "List of catalog entries must be a valid list, even if empty");
        this.entries = entries;
    }

    /**
     * @return int return the page
     */
    public int getPage() {
        return page;
    }


    /**
     * @return int return the nextPage
     */
    public int getNextPage() {
        return nextPage;
    }


    /**
     * @return int return the total
     */
    public int getTotal() {
        return total;
    }

    /**
     * @return List<CatalogEntry> return the entries
     */
    public List<CatalogEntry> getEntries() {
        return entries;
    }

}
