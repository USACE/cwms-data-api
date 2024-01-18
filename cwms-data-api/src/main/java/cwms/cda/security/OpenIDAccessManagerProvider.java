package cwms.cda.security;

import cwms.cda.spi.AccessManagerProvider;
import cwms.cda.spi.CdaAccessManager;

public class OpenIDAccessManagerProvider implements AccessManagerProvider {
    public static final String WELL_KNOWN_PROPERTY = "cwms.dataapi.access.openid.wellKnownUrl";
    public static final String ALT_AUTH_URL = "cwms.dataapi.access.openid.altAuthUrl";
    public static final String ISSUER_PROPERTY = "cwms.dataapi.access.openid.issuer";
    public static final String TIMEOUT_PROPERTY = "cwms.dataapi.access.openid.timeout";

    @Override
    public String getName() {
        return "OpenID";
    }

    @Override
    public CdaAccessManager create() {
        String wellKnownUrl = System.getProperty(WELL_KNOWN_PROPERTY,System.getenv(WELL_KNOWN_PROPERTY));
        String issuer = System.getProperty(ISSUER_PROPERTY,System.getenv(ISSUER_PROPERTY));
        String timeoutStr = System.getProperty(TIMEOUT_PROPERTY,System.getenv(TIMEOUT_PROPERTY));
        String altAuthUrl = System.getProperty(ALT_AUTH_URL, System.getenv(ALT_AUTH_URL));
        int timeout = 3600; 
        if (timeoutStr != null && !timeoutStr.isEmpty()) {
            timeout = Integer.parseInt(timeoutStr);
        }
        return new OpenIDAccessManager(wellKnownUrl,issuer,timeout,altAuthUrl);
    }

}
