import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/api-keys",
  workers: 1,
  use: { baseURL: "http://127.0.0.1:5178", browserName: "chromium" },
  webServer: {
    command:
      "npx vite --mode dev-cda-compose --host 127.0.0.1 --port 5178 --strictPort",
    url: "http://127.0.0.1:5178/cwms-data/",
    timeout: 120000,
    env: {
      VITE_CDA_API_ROOT: "/cwms-data",
      VITE_AUTH_HOST: "/auth",
      VITE_AUTH_REALM: "cwms",
      VITE_AUTH_USER: "test-user",
      VITE_AUTH_PASSWORD: "test-only",
    },
  },
});
