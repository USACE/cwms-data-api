import SwaggerUIBundle from "swagger-ui-dist/swagger-ui-bundle";
import "swagger-ui-dist/swagger-ui.css";

import { useEffect } from "react";

export default function SwaggerUI() {
  useEffect(() => {
    // Alter the page title to match the swagger page
    document.title = "CWMS Data API for Data Retrieval - Swagger UI";
    // Begin Swagger UI call region
    SwaggerUIBundle({
      url: "https://cwms-data.usace.army.mil/cwms-data/swagger-docs",
      dom_id: "#swagger-ui",
      configUrl: "./swagger-config.yaml",
      deepLinking: false,
      presets: [SwaggerUIBundle.presets.apis],
      plugins: [SwaggerUIBundle.plugins.DownloadUrl],
    });
  }, []);

  return <div id="swagger-ui"></div>;
}
