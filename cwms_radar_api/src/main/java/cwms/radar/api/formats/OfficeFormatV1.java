package cwms.radar.api.formats;

import java.util.HashMap;
import java.util.List;

import cwms.radar.data.dao.Office;

public class OfficeFormatV1 {
    public class OfficesFMT{        
        public List<Office> offices;
    }
    
    
    public OfficesFMT offices = new OfficesFMT();
    //public HashMap<String,HashMap<String,Office> > offices;

    public OfficeFormatV1(List<Office> offices ){
        this.offices.offices = offices;
    }
}
