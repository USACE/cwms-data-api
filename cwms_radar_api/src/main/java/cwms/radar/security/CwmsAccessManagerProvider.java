package cwms.radar.security;

import cwms.radar.spi.AccessManagerProvider;
import io.javalin.core.security.AccessManager;

public class CwmsAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "CwmsAccessManager";
    }

    @Override
    public AccessManager create() {        
        return new CwmsAccessManager();
    }
    
}
