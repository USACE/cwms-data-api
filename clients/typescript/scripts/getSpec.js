const fs = require("node:fs/promises");

const DEFAULT_SCHEMA_URL =
  "https://cwms-data.usace.army.mil/cwms-data/swagger-docs";
const OUTPUT_PATH = "cwms-swagger-raw.json";

async function main() {
  const schemaUrl = process.env.CWMS_SCHEMA_URL || DEFAULT_SCHEMA_URL;
  const response = await fetch(schemaUrl, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download schema: ${response.status} ${response.statusText}`);
  }

  const body = await response.text();
  let parsed;

  try {
    parsed = JSON.parse(body);
  } catch (error) {
    throw new Error(`Schema response was not valid JSON: ${error.message}`);
  }

  await fs.writeFile(OUTPUT_PATH, `${JSON.stringify(parsed)}\n`, "utf8");

  const schemaVersion = parsed?.info?.version || "unknown";
  console.log(`Saved ${schemaUrl} to ${OUTPUT_PATH} (schema version ${schemaVersion})`);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
