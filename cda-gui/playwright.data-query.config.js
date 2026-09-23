import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/data-query",
  workers: 1,
  use: { baseURL: "http://127.0.0.1:5184", browserName: "chromium" },
  webServer: {
    command:
      "npx vite --mode dev-cda-compose --host 127.0.0.1 --port 5184 --strictPort",
    url: "http://127.0.0.1:5184/cwms-data/",
    timeout: 120000,
    env: { VITE_CDA_API_ROOT: "/cwms-data" },
  },
});
