package cwms.cda.formatters;

import cwms.cda.data.dto.LocationGroup;
import java.util.List;


public class LocationGroupFormatV1 {
    private List<LocationGroup> locationGroups;

    public LocationGroupFormatV1(List<LocationGroup> locationGroups) {
        this.locationGroups = locationGroups;
    }

    public List<LocationGroup> getLocationGroups() {
        return locationGroups;
    }

    public void setLocationGroups(List<LocationGroup> locationGroups) {
        this.locationGroups = locationGroups;
    }
}
