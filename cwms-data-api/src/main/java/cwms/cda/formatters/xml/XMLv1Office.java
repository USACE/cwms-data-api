package cwms.cda.formatters.xml;

import java.util.List;

import javax.xml.bind.annotation.*;

import com.fasterxml.jackson.annotation.JsonRootName;
import cwms.cda.data.dto.Office;

@JsonRootName("offices")
public class XMLv1Office {
    List<Office> offices;


    public XMLv1Office(){}

    public XMLv1Office(List<Office> offices){
        this.offices = offices;
    }
}
