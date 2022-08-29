package cwms.radar.spi;

import io.javalin.core.security.AccessManager;

public interface AccessManagerProvider {
    public String getName();
    public AccessManager create();
}
