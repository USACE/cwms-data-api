package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;

public class OpenIDAccessManagerProvider implements AccessManagerProvider {
    public static final String WELL_KNOWN_PROPERTY = "cwms.dataapi.access.openid.wellKnownUrl";
    public static final String ISSUER_PROPERTY = "cwms.dataapi.access.openid.issuer";
    public static final String TIMEOUT_PROPERTY = "cwms.dataapi.access.openid.timeout";

    @Override
    public String getName() {
        return "OpenID";
    }

    @Override
    public RadarAccessManager create() {
        String wellKnownUrl = System.getProperty(WELL_KNOWN_PROPERTY,System.getenv(WELL_KNOWN_PROPERTY));
        String issuer = System.getProperty(ISSUER_PROPERTY,System.getenv(ISSUER_PROPERTY));
        String timeoutStr = System.getProperty(TIMEOUT_PROPERTY,System.getenv(TIMEOUT_PROPERTY));
        int timeout = 3600; 
        if (timeoutStr != null && !timeoutStr.isEmpty()) {
            timeout = Integer.parseInt(timeoutStr);
        }
        return new OpenIDAccessManager(wellKnownUrl,issuer,timeout);
    }

}
