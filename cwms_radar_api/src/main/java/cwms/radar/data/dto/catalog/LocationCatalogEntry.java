package cwms.radar.data.dto.catalog;

import java.util.List;
import java.util.Objects;

import javax.xml.bind.annotation.*;

public class LocationCatalogEntry extends CatalogEntry{        
    private String name;
    private String nearestCity;
    @XmlElementWrapper(name="aliases")
    @XmlElement(name="alias")
    private List<LocationAlias> aliases;   

    private LocationCatalogEntry(){ super(null);}

    public LocationCatalogEntry(String office, String name, String nearestCity, List<LocationAlias> aliases){
        super(office);
        this.name=name;
        this.nearestCity = nearestCity;
        Objects.requireNonNull(aliases, "alaies provided must be an actual list, empty list is okay");
        this.aliases = aliases;
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
        for( LocationAlias alias: aliases){
            builder.append(";alias=").append(alias.toString());
        }        
        return builder.toString();
    }    
}
