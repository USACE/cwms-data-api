package cwms.radar.security;

import java.util.ArrayList;

import javax.management.ServiceNotFoundException;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.AccessManagers;
import cwms.radar.spi.RadarAccessManager;

public class MultipleAccessManagerProvider implements AccessManagerProvider {
    private static final String PROVIDERS_LIST_KEY = "radar.access.providers";

    @Override
    public String getName() {
        return "MultipleAccessManager";
    }

    @Override
    public RadarAccessManager create() {        
        ArrayList<RadarAccessManager> managers = new ArrayList<>();
        String managersString = System.getProperty(PROVIDERS_LIST_KEY,System.getenv(PROVIDERS_LIST_KEY));
        if (managersString == null) {
            throw new RuntimeException("radar.access.providers property MUST be set to use the MultipleAccessProvider");
        }
        AccessManagers am = new AccessManagers();
        for(String provider: managersString.split(",")) {
            try {
                managers.add(am.get(provider));
            } catch (ServiceNotFoundException e) {
                throw new RuntimeException("Unable to initialize provider '" + provider + "'",e);
            }
        }

        return new MultipleAccessManager(managers);
    }
    
}
