package cwms.cda.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

public class CdaIdentityProviders {
    
    private static final ServiceLoader<IdentityProvider> loader = ServiceLoader.load(IdentityProvider.class);


    private CdaIdentityProviders() {
        loader.reload();
        /* Factory class */
    }

    public static Iterator<IdentityProvider> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    public static Iterator<IdentityProvider> providers() {
        return providers(false);
    }
}
