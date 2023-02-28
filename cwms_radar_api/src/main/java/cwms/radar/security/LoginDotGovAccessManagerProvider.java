package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class LoginDotGovAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "LoginDotGov";
    }

    @Override
    public RadarAccessManager create() {
        return new LoginDotGovAccessManager();
    }

}
