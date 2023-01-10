package cwms.radar.spi;

import io.javalin.core.security.AccessManager;

import io.swagger.v3.oas.models.security.SecurityScheme;

public abstract class RadarAccessManager implements AccessManager {
    public abstract SecurityScheme getScheme();
}
