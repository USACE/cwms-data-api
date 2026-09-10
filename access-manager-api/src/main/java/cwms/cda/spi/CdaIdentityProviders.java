package cwms.cda.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

public class CdaIdentityProviders {

    private static final ServiceLoader<IdentityProvider> loader = ServiceLoader.load(IdentityProvider.class);


    private CdaIdentityProviders() {
        loader.reload();
        /* Factory class */
    }

    /**
     * Retrieve iterator of providers, optionally reloading from the class path.
     * @param refresh whether to reload from the class path
     * @return known identity providers
     */
    public static Iterator<IdentityProvider> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    /**
     * Retrieve loaded providers.
     * @return known identity providers
     */
    public static Iterator<IdentityProvider> providers() {
        return providers(false);
    }
}
