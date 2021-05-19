package cwms.radar.data.dto.catalog;

public class LocationCatalogEntry extends CatalogEntry{        
    private String name;
    private String nearestCity;

    public LocationCatalogEntry(String office, String name, String nearestCity){
        super(office);
        this.name=name;
        this.nearestCity = nearestCity;
    }    
        
    public String getName(){
        return name;
    }

    public String getNearestCity(){
        return nearestCity;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(getOffice()).append("/").append(name).append(";nearestCity=").append(nearestCity);  
        return builder.toString();
    }    
}
