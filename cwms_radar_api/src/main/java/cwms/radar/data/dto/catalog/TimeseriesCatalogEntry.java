package cwms.radar.data.dto.catalog;

public class TimeseriesCatalogEntry extends CatalogEntry{        
    private String tsName;
    private String units;

    public TimeseriesCatalogEntry(String office, String name, String units){
        super(office);
        this.tsName=name;
        this.units = units;
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
        builder.append(getOffice()).append("/").append(tsName).append(";units=").append(units);        
        return builder.toString();
    }    
}
