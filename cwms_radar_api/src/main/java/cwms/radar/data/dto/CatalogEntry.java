package cwms.radar.data.dto;

public class CatalogEntry {    
    private String office;
    private String tsName;
    private String units;

    public CatalogEntry(String office, String name, String units){
        this.office = office;
        this.tsName=name;
        this.units = units;

    }

    public String getOffice(){
        return office;
    }
    
    public String getFullName(){
        return tsName;
    }

    public String getUnits(){
        return units;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(office).append("/").append(tsName).append(";units=").append(units);        
        return builder.toString();
    }
}
