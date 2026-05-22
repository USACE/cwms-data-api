const fs = require("node:fs");
const path = require("node:path");

const rootDir = path.resolve(__dirname, "..", "..");

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(rootDir, relativePath), "utf8"));
}

function writeJson(relativePath, value) {
  fs.writeFileSync(
    path.join(rootDir, relativePath),
    `${JSON.stringify(value, null, 2)}\n`,
    "utf8",
  );
}

function getVersionSuffix(rawSpec) {
  return rawSpec?.info?.version || new Date().toISOString().slice(0, 10).replace(/-/g, ".");
}

function removeUniqueItems(value) {
  if (Array.isArray(value)) {
    return value.map(removeUniqueItems);
  }

  if (value && typeof value === "object") {
    const result = {};
    for (const [key, childValue] of Object.entries(value)) {
      if (key === "uniqueItems") {
        continue;
      }
      result[key] = removeUniqueItems(childValue);
    }
    return result;
  }

  return value;
}

function replaceInString(value) {
  return value
    .replaceAll("timeseries", "time-series")
    .replaceAll("Timeseries", "TimeSeries")
    .replaceAll(
      "#/components/schemas/AbstractRatingMetadata",
      "#/components/schemas/BaseRatingMetadata",
    );
}

function normalizeStrings(value) {
  if (Array.isArray(value)) {
    return value.map(normalizeStrings);
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, childValue]) => [
        replaceInString(key),
        normalizeStrings(childValue),
      ]),
    );
  }

  return typeof value === "string" ? replaceInString(value) : value;
}

function stripCwmsDataPrefix(spec) {
  const paths = {};
  for (const [route, routeConfig] of Object.entries(spec.paths || {})) {
    const normalizedRoute = route.replace(/^\/cwms-data(?=\/|$)/, "") || "/";
    paths[normalizedRoute] = routeConfig;
  }
  spec.paths = paths;
  spec.servers = (spec.servers || []).map((server) => ({
    ...server,
    url: server.url.replace(/\/cwms-data\/?$/, ""),
  }));
}

function normalizeOperationIds(spec) {
  for (const pathItem of Object.values(spec.paths || {})) {
    if (!pathItem || typeof pathItem !== "object") {
      continue;
    }

    for (const operation of Object.values(pathItem)) {
      if (!operation || typeof operation !== "object" || !operation.operationId) {
        continue;
      }

      operation.operationId = operation.operationId.replace(
        /^(get|post|patch|put|delete)CwmsData(\w*)$/,
        "$1$2",
      );
    }
  }
}

function main() {
  const rawSpec = readJson("cwms-swagger-raw.json");
  const rootPackage = readJson("package.json");
  const servers = readJson("scripts/spec-updates/servers.json");
  const baseRatingMetadata = readJson("scripts/spec-updates/BaseRatingMetadata.json");
  const tsArrayItems = readJson("scripts/spec-updates/tsArrayItems.json");

  const schemaVersion = getVersionSuffix(rawSpec);
  const spec = structuredClone(rawSpec);

  spec.servers = servers.servers;
  spec.info.version = `${rootPackage.version}-${schemaVersion}`;
  spec.components ??= {};
  spec.components.schemas ??= {};
  spec.components.schemas.BaseRatingMetadata = baseRatingMetadata.BaseRatingMetadata;

  if (
    spec.components.schemas.TimeSeries?.properties?.values?.items &&
    spec.components.schemas.TimeSeries.properties.values.items.type === "array"
  ) {
    spec.components.schemas.TimeSeries.properties.values.items.items = tsArrayItems.items;
  }

  if (spec.paths?.["/cwms-data/offices"]?.get?.responses?.["200"]?.content?.["application/json"]) {
    spec.paths["/cwms-data/offices"].get.responses["200"].content[""] =
      spec.paths["/cwms-data/offices"].get.responses["200"].content["application/json"];
  }

  const cleaned = removeUniqueItems(spec);
  const normalized = normalizeStrings(cleaned);
  normalizeOperationIds(normalized);
  stripCwmsDataPrefix(normalized);

  writeJson("cwms-swagger-mod.json", normalized);
}

main();
