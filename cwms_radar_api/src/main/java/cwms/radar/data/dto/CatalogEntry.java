package cwms.radar.data.dto;

public class CatalogEntry {    
    private String office;
    private String tsName;

    public CatalogEntry(String office, String name){
        this.office = office;
        this.tsName=name;

    }

    public String getOffice(){
        return office;
    }
    
    public String getFullName(){
        return tsName;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(office).append("/").append(tsName);        
        return builder.toString();
    }
}
