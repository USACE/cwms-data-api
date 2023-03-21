package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class OpenIDAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "LoginDotGov";
    }

    @Override
    public RadarAccessManager create() {
        String realmUrl = System.getProperty("cwms.dataapi.access.openid.realm");
        String issuer = System.getProperty("cwms.dataapi.access.openid.issuer");
        int timeout = Integer.parseInt(System.getProperty("cwms.dataapi.access.openid.timeout","3600"));
        return new OpenIDAccessManager(realmUrl,issuer,timeout);
    }

}
