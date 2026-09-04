import { Link } from "react-router-dom";
import { routePaths } from "../route-paths";

const labels = {
  home: "Home",
  "quick-start": "Getting Started",
  disclaimer: "Disclaimer",
  "site-map": "Site Map",
  "swagger-ui": "Swagger UI (developer documentation)",
  "data-query": "Data Query",
  regexp: "Regular Expressions",
  "filter-expressions": "Filter Expressions (RSQL)",
  timestamps: "Timestamps",
  "user-lists": "User Lists",
  "legacy-format": "Legacy Formats",
  "location-search": "Location Search",
};

export default function SiteMap() {
  return (
    <article className="mx-auto max-w-4xl py-8">
      <h1 className="text-3xl font-bold mb-6">Site Map</h1>
      <ul className="list-disc pl-6 space-y-3">
        {routePaths.map(({ id, sitemapPath }) => (
          <li key={id}>
            <Link className="underline" to={`/${sitemapPath}`}>
              {labels[id] || id}
            </Link>
          </li>
        ))}
      </ul>
    </article>
  );
}
