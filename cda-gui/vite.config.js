import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const BASE_PATH = env.VITE_BASE_PATH || "/cwms-data";
  const CDA_PATH = env.VITE_CDA_URL || BASE_PATH;
  const proxyTarget = env.VITE_CDA_PROXY_TARGET || "https://cwms-data.usace.army.mil";

  return {
    base: BASE_PATH,
    plugins: [react()],
    server: {
      proxy: {
        [`^${CDA_PATH}/timeseries/.*`]: {
          target: proxyTarget,
          changeOrigin: true,
          secure: false,
        },
        [`^${CDA_PATH}/catalog/.*`]: {
          target: proxyTarget,
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
