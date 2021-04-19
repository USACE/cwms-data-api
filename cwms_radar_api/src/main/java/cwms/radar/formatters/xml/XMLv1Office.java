package cwms.radar.formatters.xml;

import java.util.List;

import javax.xml.bind.annotation.*;

import cwms.radar.data.dao.Office;

@XmlRootElement(name = "offices")
@XmlAccessorType (XmlAccessType.FIELD)
public class XMLv1Office {
    @XmlElementWrapper(name="offices")
    @XmlElement(name="office")
    List<Office> offices;


    public XMLv1Office(){}

    public XMLv1Office(List<Office> offices){
        this.offices = offices;
    }
}
