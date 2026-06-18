import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const cdaApiRoot = new URL(env.CDA_API_ROOT || "http://localhost:8081").origin;
  // const BASE_PATH = env?.BASE_PATH ?? "/cwms-data";
  return {
    base: "/cwms-data",
    plugins: [react()],
    server: {
      proxy: {
        "^/(auth|CWMSLogin|cwms-data/(?!swagger-ui(?:/|$)|assets/|src/|node_modules/|@vite/).*)":
          {
            target: cdaApiRoot,
            changeOrigin: true,
            secure: false,
          },
      },
    },
    experimental: {
      renderBuiltUrl(filename, { hostType }) {
        if (hostType === "js" || hostType === "css") {
          return { runtime: `window.__toCdnUrl(${JSON.stringify(filename)})` };
        } else {
          return { relative: true };
        }
      },
    },
  };
});
