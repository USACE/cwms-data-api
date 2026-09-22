import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/user-roles",
  use: { baseURL: "http://127.0.0.1:18742", headless: true },
  workers: 1,
  webServer: {
    command: "npm run dev:onboarding",
    url: "http://127.0.0.1:18742/cwms-data/",
    reuseExistingServer: true,
  },
});
