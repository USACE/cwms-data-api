import { cwd } from "node:process";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, cwd(), "");
  const cdaApiRoot = new URL(
    env.VITE_CDA_API_ROOT || env.CDA_API_ROOT || "http://localhost:8081",
    "http://localhost:8081",
  ).origin;
  // const BASE_PATH = env?.BASE_PATH ?? "/cwms-data";
  return {
    base: "/cwms-data",
    plugins: [react()],
    server: {
      proxy: {
        "^/(auth|CWMSLogin|cwms-data/(?!$|swagger-ui(?:/|$)|data-query(?:/|$)|regexp(?:/|$)|filter-expressions(?:/|$)|timestamps(?:/|$)|user-lists(?:/|$)|legacy-format(?:/|$)|location-search(?:/|$)|quick-start(?:/|$)|disclaimer(?:/|$)|site-map(?:/|$)|assets/|src/|node_modules/|@).*)":
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
