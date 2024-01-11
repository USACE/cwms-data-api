package cwms.cda.data.dto;

import java.util.HashMap;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.csv.CsvV1;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.tab.TabV1;
import cwms.cda.formatters.xml.XMLv1;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a CWMS office")
@XmlRootElement(name="office")
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.XML, formatter = XMLv1.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@FormattableWith(contentType = Formats.CSV, formatter = CsvV1.class)
@FormattableWith(contentType = Formats.TAB, formatter = TabV1.class)
public class Office implements CwmsDTOBase {
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
