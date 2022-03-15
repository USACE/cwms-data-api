package cwms.radar.data.dto;

import java.util.HashMap;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import cwms.radar.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a CWMS office")
@XmlRootElement(name="office")
@XmlAccessorType(XmlAccessType.FIELD)
public class Office implements CwmsDTO{
    private static final HashMap<String,String> office_types = new HashMap<String,String>(){
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        {
            put("UNK","unknown");
            put("HQ","corps headquarters");
            put("MSC","division headquarters");
            put("MSCR","division regional");
            put("DIS","district");
            put("FOA","field operating activity");
        }
    };

    private String name;
    @XmlElement(name="long-name")
    private String longName;
    @Schema(allowableValues = {"unknown","corps headquarters","division headquarters","division regional","district","filed operating activity"})
    private String type;
    @XmlElement(name="reports-to")
    @Schema(description = "Reference to another office, like a division, that this office reports to.")
    private String reportsTo;

    public Office(){}

    public Office(String name, String longName, String officeType, String reportsTo ){
        this.name = name;
        this.longName = longName;
        this.type = office_types.get(officeType);
        this.reportsTo = reportsTo;
    }

    public String getName(){ return name; }
    public String getLongName(){ return longName; }
    public String getType(){ return type; }
    public String getReportsTo(){ return reportsTo; }

    public static boolean validOfficeNotNull(String office){
        return office !=null && office.matches("^[a-zA-Z0-9]*$");
    }

    public static boolean validOfficeCanNull(String office){
        return office == null || validOfficeNotNull(office);
    }

    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }
}
