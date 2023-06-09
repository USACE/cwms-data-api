package cwms.radar.security;

import java.util.ArrayList;
import java.util.logging.Logger;

import javax.management.ServiceNotFoundException;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.AccessManagers;
import cwms.radar.spi.RadarAccessManager;

public class MultipleAccessManagerProvider implements AccessManagerProvider {
    private static final String PROVIDERS_LIST_KEY_OLD = "radar.access.providers";
    private static final String PROVIDERS_LIST_KEY = "cwms.dataapi.access.providers";
    private static final Logger log = Logger.getLogger(MultipleAccessManager.class.getName());

    @Override
    public String getName() {
        return "MultipleAccessManager";
    }

    @Override
    public RadarAccessManager create() {        
        ArrayList<RadarAccessManager> managers = new ArrayList<>();
        managers.add(new GuestAccessManager());
        String managersString = System.getProperty(
                                    PROVIDERS_LIST_KEY,
                                    System.getProperty(
                                        PROVIDERS_LIST_KEY_OLD,
                                        System.getenv(PROVIDERS_LIST_KEY)
                                        )
                                );
        if (managersString == null) {
            // one last variable to check
            managersString = System.getenv(PROVIDERS_LIST_KEY_OLD);
        }
        if (managersString == null) {
            log.info("No additional access managers provided. Defaulting to readonly.");
            log.info(() -> "Set environment property '" + PROVIDERS_LIST_KEY
                         + "' to comma seperated list of desired Managers if this is incorrect.");
        } else {
            AccessManagers am = new AccessManagers();
            for(String provider: managersString.split(",")) {
                try {
                    managers.add(am.get(provider));
                } catch (ServiceNotFoundException e) {
                    throw new RuntimeException("Unable to initialize provider '" + provider + "'",e);
                }
            }
        }
        // Always add this to the end of the list to cover edge cases.
        managers.add(new NoAccessManager());
        return new MultipleAccessManager(managers);
    }
    
}
