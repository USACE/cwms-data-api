package cwms.cda.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

import javax.management.ServiceNotFoundException;

import org.jetbrains.annotations.NotNull;

public class AccessManagers {
    
    ServiceLoader<AccessManagerProvider> loader = ServiceLoader.load(AccessManagerProvider.class);

    public Iterator<AccessManagerProvider> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    public CdaAccessManager get(@NotNull String name) throws ServiceNotFoundException {
        CdaAccessManager am[] = new CdaAccessManager[1];
        this.loader.forEach((amp)-> {
            if (name.equalsIgnoreCase(amp.getName())) {
                am[0] = amp.create();                
            }
        });
        if (am[0] == null) {
            throw new ServiceNotFoundException("No service by the name " + name + " is available");
        }
        return am[0];
    }
}
