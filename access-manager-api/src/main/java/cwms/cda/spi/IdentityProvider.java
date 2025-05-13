package cwms.cda.spi;

import java.security.Principal;

import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;

public interface IdentityProvider {
    public static final String PRINCIPAL_KEY = "prinicipal";
    /**
     * Key used in OpenAPI definition to distinguish Auth types
     * @return
     */
    String getName();
    boolean canAuth(Context ctx);
    Principal authenticate(Context ctx);
    /**
     * Define the OpenAPI V3 Security Scheme for this manager
     * @return
     */
    SecurityScheme getScheme();
}
