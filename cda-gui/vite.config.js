import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  // const BASE_PATH = env?.BASE_PATH ?? "/cwms-data";
  return {
    base: "/cwms-data",
    plugins: [react()],
    server: {
      proxy: {
        "^/cwms-data/timeseries/.*": {
          target: env.CDA_API_ROOT,
          changeOrigin: true,
          secure: false,
        },
        "^/cwms-data/catalog/.*": {
          target: env.CDA_API_ROOT,
          changeOrigin: true,
          secure: false,
        },
        "^/cwms-data/auth/.*": {
          target: env.CDA_API_ROOT,
          changeOrigin: true,
          secure: false,
        },
        "^/cwms-data/swagger-docs$": {
          target: env.CDA_API_ROOT,
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
