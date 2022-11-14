package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class KeyAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "KeyAccessManager";
    }

    @Override
    public RadarAccessManager create() {   
        return new KeyAccessManager();
    }
    
}
