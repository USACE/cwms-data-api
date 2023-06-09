package cwms.cda.security;

import cwms.cda.spi.AccessManagerProvider;
import cwms.cda.spi.CdaAccessManager;

public class KeyAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "KeyAccessManager";
    }

    @Override
    public CdaAccessManager create() {   
        return new KeyAccessManager();
    }
    
}
