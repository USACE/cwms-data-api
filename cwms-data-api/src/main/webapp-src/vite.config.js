import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'


// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react()],
    experimental: {
        renderBuiltUrl(filename, { hostType }) {
            console.log('render url', filename)
            if (hostType === 'js' || hostType === 'css') {
                return { runtime: `window.__toCdnUrl(${JSON.stringify(filename)})` }
            } else {
                return { relative: true }
            }
        },
    },
})
