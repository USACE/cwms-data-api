package cwms.cda.formatters;

import cwms.cda.data.dto.Office;
import java.util.List;

public class OfficeFormatV1 {
    public static class OfficesFMT {
        public List<Office> offices;
    }
    
    
    public OfficesFMT offices = new OfficesFMT();    

    public OfficeFormatV1(List<Office> offices) {
        this.offices.offices = offices;
    }
}
