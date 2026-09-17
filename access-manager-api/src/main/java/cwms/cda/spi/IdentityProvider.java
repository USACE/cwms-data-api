package cwms.cda.spi;

import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.security.Principal;


public interface IdentityProvider {
    public static final String PRINCIPAL_KEY = "principal";

    /**
     * Key used in OpenAPI definition to distinguish Auth types.
     *
     * @return name name of this providered.
     */
    String getName();

    /**
     * If this provider can authenticate the given request context.
     * @param ctx Javalin Request Context
     * @return if the provider can process the given request.
     */
    boolean canAuth(Context ctx);

    /**
     * Create the Prinicpal object given the information in the request Context.
     * @param ctx Javalin Request Context
     * @return principal object never null, implementations should throw CwmsAuthException 
     *         (a runtime exception) on any errors.
     */
    Principal authenticate(Context ctx);

    /**
     * Define the OpenAPI V3 Security Scheme for this manager.
     *
     * @return SecurityScheme or null if the scheme should not be active at this time.
     */
    SecurityScheme getScheme();
}
