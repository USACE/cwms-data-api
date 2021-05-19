package cwms.radar.data.dto.catalog;

public abstract class CatalogEntry {
    private String office;


    public CatalogEntry(String office){
        this.office = office;
    }

    public String getOffice(){
        return office;
    }
    

}
