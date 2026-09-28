import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/auth",
  use: { baseURL: "http://127.0.0.1:18741", headless: true },
  webServer: {
    command: "npx vite --mode test --host 127.0.0.1 --port 18741 --strictPort",
    url: "http://127.0.0.1:18741/cwms-data/",
  },
});
