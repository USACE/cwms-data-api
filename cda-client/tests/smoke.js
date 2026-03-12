import { readFile } from "node:fs/promises";
import {
  CatalogApi,
  Configuration,
  OfficesApi,
  ProjectsApi,
} from "../cwmsjs/dist/index.js";

if (!global.fetch) {
  throw new Error("This smoke test expects a Node.js runtime with native fetch support.");
}

async function runStep(name, fn) {
  process.stdout.write(`${name}... `);
  const result = await fn();
  console.log("ok");
  return result;
}

async function main() {
  const packageJson = JSON.parse(
    await readFile(new URL("../cwmsjs/package.json", import.meta.url), "utf8")
  );
  const rootPackageJson = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8")
  );

  let expectedVersion = process.env.EXPECTED_CWMSJS_VERSION;
  try {
    const rawSpec = JSON.parse(
      await readFile(new URL("../cwms-swagger-raw.json", import.meta.url), "utf8")
    );
    expectedVersion = `${rootPackageJson.version}-${rawSpec?.info?.version}`;
  } catch {}

  if (!expectedVersion) {
    throw new Error(
      "Unable to determine expected cwmsjs version. Run buildApi first or set EXPECTED_CWMSJS_VERSION."
    );
  }

  if (packageJson.version !== expectedVersion) {
    throw new Error(
      `cwmsjs version mismatch. Expected ${expectedVersion}, found ${packageJson.version}.`
    );
  }

  console.log(`cwmsjs version: ${packageJson.version}`);

  const config = new Configuration({
    basePath: "https://cwms-data.usace.army.mil/cwms-data",
  });
  const officesApi = new OfficesApi(config);
  const catalogApi = new CatalogApi(config);
  const projectsApi = new ProjectsApi(config);

  await runStep("offices", async () => {
    const offices = await officesApi.getOffices();
    console.log(`  offices returned: ${offices?.offices?.length ?? offices?.length ?? 0}`);
  });

  await runStep("catalog", async () => {
    const catalog = await catalogApi.getCatalogWithDataset({
      office: "SWT",
      like: "KEYS..*.Inst.1Hour.0.Ccp-Rev",
      dataset: "TIMESERIES",
    });
    console.log(`  catalog entries returned: ${catalog?.entries?.length ?? 0}`);
  });

  await runStep("projects", async () => {
    const projects = await projectsApi.getProjects({
      office: "SWT",
      idMask: "KEYS*",
      pageSize: 5,
    });
    console.log(`  projects returned: ${projects?.projects?.length ?? 0}`);
  });
}

main().catch(async (error) => {
  if (error?.response) {
    const body = await error.response.text();
    console.error(error.response.url);
    console.error(body);
  } else {
    console.error(error);
  }
  process.exit(1);
});
