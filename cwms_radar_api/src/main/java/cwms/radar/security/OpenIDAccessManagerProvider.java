package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class OpenIDAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "OpenID";
    }

    @Override
    public RadarAccessManager create() {
        String wellKnownUrl = System.getProperty("cwms.dataapi.access.openid.wellKnownUrl");
        String issuer = System.getProperty("cwms.dataapi.access.openid.issuer");
        int timeout = Integer.parseInt(System.getProperty("cwms.dataapi.access.openid.timeout","3600"));
        return new OpenIDAccessManager(wellKnownUrl,issuer,timeout);
    }

}
