import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { sitemapPaths } from "../src/route-paths.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectDir = path.resolve(__dirname, "..");
const outputPath = path.join(projectDir, "dist", "sitemap.xml");

const siteOrigin = (
  process.env.SITE_ORIGIN ?? "https://cwms-data.usace.army.mil"
).replace(/\/+$/, "");
const siteBasePath = (process.env.SITE_BASE_PATH ?? "").replace(/\/+$/, "");

const urls = sitemapPaths.map((routePath) => {
  const normalizedPath = routePath ? `/${routePath}` : "";
  return `${siteOrigin}${siteBasePath}${normalizedPath}`;
});

const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls
  .map(
    (url) => `  <url>
    <loc>${url}</loc>
  </url>`,
  )
  .join("\n")}
</urlset>
`;

await mkdir(path.dirname(outputPath), { recursive: true });
await writeFile(outputPath, xml, "utf8");
