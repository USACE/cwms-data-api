package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dao.Office;

public class OfficeFormatV1 {
    public class OfficesFMT{        
        public List<Office> offices;
    }
    
    
    public OfficesFMT offices = new OfficesFMT();    

    public OfficeFormatV1(List<Office> offices ){
        this.offices.offices = offices;
    }
}
