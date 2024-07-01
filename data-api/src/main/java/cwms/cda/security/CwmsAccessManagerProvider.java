package cwms.cda.security;

import cwms.cda.spi.AccessManagerProvider;
import cwms.cda.spi.CdaAccessManager;

public class CwmsAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "CwmsAccessManager";
    }

    @Override
    public CdaAccessManager create() {
        return new CwmsAccessManager();
    }
}
