package cwms.radar.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

import javax.management.ServiceNotFoundException;

import org.jetbrains.annotations.NotNull;

import io.javalin.core.security.AccessManager;

public class AccessManagers {
    
    ServiceLoader<AccessManagerProvider> loader = ServiceLoader.load(AccessManagerProvider.class);

    public Iterator<AccessManagerProvider> providers(boolean refresh) {
        if (refresh) {
            loader.reload();
        }
        return loader.iterator();
    }

    public AccessManager get(@NotNull String name) throws ServiceNotFoundException {
        AccessManager am[] = new AccessManager[1];
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
