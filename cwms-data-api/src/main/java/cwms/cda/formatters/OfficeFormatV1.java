package cwms.cda.formatters;

import java.util.List;

import cwms.cda.data.dto.Office;

public class OfficeFormatV1 {
    public static class OfficesFMT{
        public List<Office> offices;
    }
    
    
    public OfficesFMT offices = new OfficesFMT();    

    public OfficeFormatV1(List<Office> offices ){
        this.offices.offices = offices;
    }
}
