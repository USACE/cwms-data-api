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
        "/cwms-data/timeseries": {
          target: cdaApiRoot,
          changeOrigin: true,
          secure: false,
        },
        "/cwms-data/catalog": {
          target: cdaApiRoot,
          changeOrigin: true,
          secure: false,
        },
        "/cwms-data/auth": {
          target: cdaApiRoot,
          changeOrigin: true,
          secure: false,
        },
        "/auth": {
          target: cdaApiRoot,
          changeOrigin: true,
          secure: false,
        },
        "/CWMSLogin": {
          target: cdaApiRoot,
          changeOrigin: true,
          secure: false,
        },
        "/cwms-data/swagger-docs": {
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
