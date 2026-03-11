import process from "node:process";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

const normalizePath = (value, fallback = "/cwms-data") => {
  /* Returns a normalized path segment from a URL or path string. */

  const candidate = value || fallback;
  let pathname = candidate;

  try {
    pathname = new URL(candidate).pathname;
  } catch {
    // Use as-is when candidate is already a path segment.
  }
  // remove trailing slashes and ensure leading slash
  if (!pathname.startsWith("/")) pathname = `/${pathname}`;
  return pathname.replace(/\/+$/, "") || "/";
};

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const BASE_PATH = env.VITE_BASE_PATH || "/cwms-data";
  const CDA_PATH = normalizePath(env.VITE_CDA_URL, BASE_PATH);
  const proxyTarget = env.VITE_CDA_PROXY_TARGET || "https://cwms-data.usace.army.mil";
  
  return {
    base: BASE_PATH,
    plugins: [react()],
    server: {
      proxy: {
        [`^${CDA_PATH}/.*`]: {
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
