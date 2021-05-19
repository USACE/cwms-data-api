package cwms.radar.data.dto.catalog;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="entry")
@XmlAccessorType(XmlAccessType.FIELD)
public class TimeseriesCatalogEntry extends CatalogEntry{        
    @XmlAttribute
    private String tsName;
    private String units;
    
    private TimeseriesCatalogEntry(){ super(null);}

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
