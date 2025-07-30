import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  // const env = loadEnv(mode, process.cwd(), "");
  // const BASE_PATH = env?.BASE_PATH ?? "/cwms-data";
  return {
    plugins: [react()],
    define: {
      "import.meta.env.CDA_URL": JSON.stringify("/cwms-data"),
    },
    server: {
      proxy: {
        "^/cwms-data/timeseries/.*": {
          target: "https://cwms-data.usace.army.mil",
          changeOrigin: true,
          secure: false,
        },
         "^/cwms-data/catalog/.*": {
          target: "https://cwms-data.usace.army.mil",
          changeOrigin: true,
          secure: false,
        },
      },
    },
    experimental: {
      renderBuiltUrl(filename, { hostType }) {
        console.log("render url", filename);
        if (hostType === "js" || hostType === "css") {
          return { runtime: `window.__toCdnUrl(${JSON.stringify(filename)})` };
        } else {
          return { relative: true };
        }
      },
    },
  };
});
