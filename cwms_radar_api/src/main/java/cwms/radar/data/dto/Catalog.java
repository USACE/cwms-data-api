package cwms.radar.data.dto;

import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class Catalog implements CwmsDTO {
    private String page;
    private String nextPage;
    private int total;
    private List<CatalogEntry> entries;

    public Catalog(String page, int total, int pageSize, List<CatalogEntry> entries ){
        this.page = page;        
        this.total = total;

        Objects.requireNonNull(entries, "List of catalog entries must be a valid list, even if empty");
        this.entries = entries;
        if( entries.size() == pageSize){
            String nextSet = String.format("%s|||%d",entries.get(entries.size()-1).getFullName(),total);
            
            nextPage = Base64.getEncoder().encodeToString(nextSet.getBytes());
        } else {
            nextPage = null;
        }
        
    }

    /**
     * @return String return the page
     */
    public String getPage() {
        return page;
    }


    /**
     * @return String return the nextPage
     */
    public String getNextPage() {
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
