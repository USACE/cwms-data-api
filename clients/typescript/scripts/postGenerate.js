const fs = require("node:fs");
const path = require("node:path");

const rootDir = __dirname ? path.resolve(__dirname, "..") : process.cwd();

function replaceInFile(relativePath, replacements) {
  const fullPath = path.join(rootDir, relativePath);
  if (!fs.existsSync(fullPath)) {
    return;
  }

  let content = fs.readFileSync(fullPath, "utf8");
  for (const [from, to] of replacements) {
    if (!content.includes(from)) {
      continue;
    }
    content = content.replace(from, to);
  }
  fs.writeFileSync(fullPath, content, "utf8");
}

function patchTsConfig(relativePath) {
  const fullPath = path.join(rootDir, relativePath);
  if (!fs.existsSync(fullPath)) {
    return;
  }

  const tsconfig = JSON.parse(fs.readFileSync(fullPath, "utf8"));
  tsconfig.compilerOptions ??= {};
  tsconfig.compilerOptions.lib = ["es2018", "dom"];
  fs.writeFileSync(fullPath, `${JSON.stringify(tsconfig, null, 2)}\n`, "utf8");
}

replaceInFile("cwmsjs/src/models/AbstractRatingMetadata.ts", [
  [
    "            return TransitionalRatingToJSON(value);",
    "            return TransitionalRatingToJSON(value as TransitionalRating);",
  ],
  [
    "            return VirtualRatingToJSON(value);",
    "            return VirtualRatingToJSON(value as VirtualRating);",
  ],
]);

replaceInFile("cwmsjs/src/models/LocationLevel.ts", [
  [
    "    return { ...ConstantLocationLevelFromJSONTyped(json, true), ...SeasonalLocationLevelFromJSONTyped(json, true), ...TimeSeriesLocationLevelFromJSONTyped(json, true), ...VirtualLocationLevelFromJSONTyped(json, true) };",
    "    return { ...ConstantLocationLevelFromJSONTyped(json, true), ...SeasonalLocationLevelFromJSONTyped(json, true), ...TimeSeriesLocationLevelFromJSONTyped(json, true), ...VirtualLocationLevelFromJSONTyped(json, true) } as LocationLevel;",
  ],
  [
    "    return { ...ConstantLocationLevelToJSON(value), ...SeasonalLocationLevelToJSON(value), ...TimeSeriesLocationLevelToJSON(value), ...VirtualLocationLevelToJSON(value) };",
    "    return { ...ConstantLocationLevelToJSON(value as any), ...SeasonalLocationLevelToJSON(value as any), ...TimeSeriesLocationLevelToJSON(value as any), ...VirtualLocationLevelToJSON(value as any) };",
  ],
]);

replaceInFile("cwmsjs/src/runtime.ts", [
  ["export type FetchAPI = GlobalFetch['fetch'];", "export type FetchAPI = typeof fetch;"],
]);

patchTsConfig("cwmsjs/tsconfig.json");
