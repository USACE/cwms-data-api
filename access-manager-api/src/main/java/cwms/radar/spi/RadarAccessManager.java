package cwms.radar.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.javalin.core.security.AccessManager;

import io.swagger.v3.oas.models.security.SecurityScheme;

public abstract class RadarAccessManager implements AccessManager {

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
    public List<RadarAccessManager> getContainedManagers() {
        List<RadarAccessManager> list = new ArrayList<>(); // I really wish we were using Java 11+
        list.add(this);
        return list;
    }    
    
}
