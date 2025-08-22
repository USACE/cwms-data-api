package cwms.cda.formatters.xml;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import cwms.cda.data.dto.Office;

@JsonRootName("offices")
@JacksonXmlRootElement(localName = "offices")
public class XMLv1Office {
    @JacksonXmlProperty(localName = "offices")
    List<Office> offices;


    public XMLv1Office(){}

    public XMLv1Office(List<Office> offices){
        this.offices = offices;
    }

    public List<Office> getOffices() {
        return offices;
    }
}
