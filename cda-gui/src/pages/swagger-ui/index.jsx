import SwaggerUIBundle from "swagger-ui-dist/swagger-ui-bundle";
import "swagger-ui-dist/swagger-ui.css";

import { useEffect } from "react";
import { getBasePath } from "../../utils/base";

export default function SwaggerUI() {
    useEffect(() => {
        // document.querySelector("#swagger-ui").prepend(Index)
        // TODO: Add page index to top of page
        // Alter the page title to match the swagger page
        document.title = "CWMS Data API for Data Retrieval - Swagger UI";
        // Begin Swagger UI call region
        // TODO: add endpoint that dynamic returns swagger generated doc

        const ui = SwaggerUIBundle({
            url: getBasePath() + "/swagger-docs",
            
            dom_id: "#swagger-ui",
            deepLinking: false,
            presets: [SwaggerUIBundle.presets.apis],
            plugins: [SwaggerUIBundle.plugins.DownloadUrl],
            requestInterceptor: (req) => {
                // Add a cache-busting query param... but only if it's to our api. Some
                // external systems, like keycloak, don't allow random unknown parameters.
                const origin = window.location.origin;                
                const re = new RegExp(`^${origin}.*`)
                if (re.test(req.url))
                {
                    const sep = req.url.includes("?") ? "&" : "?";
                    req.url = `${req.url}${sep}_cb=${Date.now()}`;

                    // Also ask intermediaries not to serve from cache
                    req.headers["Cache-Control"] = "no-cache, no-store, max-age=0";
                    req.headers["Pragma"] = "no-cache";
                }
                return req;
            },
            onComplete: () => {
                const spec = JSON.parse(ui.spec().get("spec"));
                for (const schemeName in spec.components.securitySchemes) {
                    const scheme = spec.components.securitySchemes[schemeName];
                    if (scheme.type === "openIdConnect") {
                        let additionalParams = null;
                        let hints = scheme["x-kc_idp_hint"];
                        if (hints) {
                            additionalParams = {
                                // Since getting the interface to allow users to choose
                                // is likely impossible, we will assume the first in the list
                                // is the "primary" auth system
                                "kc_idp_hint": hints.values[0]
                            };
                        }
                        ui.initOAuth({
                            clientId: scheme["x-oidc-client-id"],
                            usePkceWithAuthorizationCodeGrant: true,
                            additionalQueryStringParams: additionalParams,
                        });
                        break;
                    }
                }
            },
        });
    }, []);

  return <div id="swagger-ui"></div>;
}
