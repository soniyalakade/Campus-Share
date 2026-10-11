import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const gatewayUrl = process.env.GATEWAY_URL || 'http://localhost:4001';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: '0.0.0.0',
    proxy: {
      '/api': {
        target: gatewayUrl,
        changeOrigin: true
      },
      '/ws': {
        target: gatewayUrl.replace(/^http/, 'ws'),
        ws: true
      }
    }
  }
});
