import SwaggerUIBundle from "swagger-ui-dist/swagger-ui-bundle";
import "swagger-ui-dist/swagger-ui.css";

import {useEffect} from "react";
import {getBasePath} from "../../utils/base";

export default function SwaggerUI() {
    useEffect(() => {
        // document.querySelector("#swagger-ui").prepend(Index)
        // TODO: Add page index to top of page
        // Alter the page title to match the swagger page
        document.title = "CWMS Data API for Data Retrieval - Swagger UI";
        // Begin Swagger UI call region
        // TODO: add endpoint that dynamic returns swagger generated doc
        SwaggerUIBundle({
            url: getBasePath() + "/swagger-docs",
            dom_id: "#swagger-ui",
            deepLinking: false,
            presets: [SwaggerUIBundle.presets.apis],
            plugins: [SwaggerUIBundle.plugins.DownloadUrl],
            requestInterceptor: (req) => {
                // Add a cache-busting query param
                const sep = req.url.includes("?") ? "&" : "?";
                req.url = `${req.url}${sep}_cb=${Date.now()}`;

                // Also ask intermediaries not to serve from cache
                req.headers["Cache-Control"] = "no-cache, no-store, max-age=0";
                req.headers["Pragma"] = "no-cache";

                return req;
            },
        });
    }, []);

    return <div id="swagger-ui"></div>;
}
