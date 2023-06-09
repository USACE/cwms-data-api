package cwms.cda.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.javalin.core.security.AccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;

public abstract class CdaAccessManager implements AccessManager {

    public abstract boolean canAuth(Context ctx, Set<RouteRole> roles);
    /**
     * Key used in OpenAPI definition to distinguish Auth types
     * @return
     */
    public abstract String getName();
    /**
     * Define the OpenAPI V3 Security Scheme for this manager
     * @return
     */
    public abstract SecurityScheme getScheme();

    /**
     * Return any managers contained by this manager. Defaults 
     * to this manager only.
     * @return
     */
    public List<CdaAccessManager> getContainedManagers() {
        List<CdaAccessManager> list = new ArrayList<>(); // I really wish we were using Java 11+
        list.add(this);
        return list;
    }    
    
}
