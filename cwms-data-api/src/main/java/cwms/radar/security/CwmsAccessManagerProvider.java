package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class CwmsAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "CwmsAccessManager";
    }

    @Override
    public RadarAccessManager create() {
        return new CwmsAccessManager();
    }
}
