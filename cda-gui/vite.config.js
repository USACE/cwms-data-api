import { defineConfig, loadEnv } from "vite";
import react from '@vitejs/plugin-react'


// https://vitejs.dev/config/
export default defineConfig(({mode}) => {
    const env = loadEnv(mode, process.cwd(), "");
    const BASE_PATH = env?.BASE_PATH ?? "/cwms-data";
    return {
        base: BASE_PATH,
        plugins: [react()],
        define: {
            "import.meta.env.BASE_PATH": JSON.stringify(BASE_PATH)
        },
        experimental: {
            renderBuiltUrl(filename, { hostType }) {
                console.log('render url', filename)
                if (hostType === 'js' || hostType === 'css') {
                    return { runtime: `window.__toCdnUrl(${JSON.stringify(filename)})` }
                } else {
                    return { relative: true }
                }
            },
        }
    }
})
