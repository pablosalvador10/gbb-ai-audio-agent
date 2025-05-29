import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react({
      // Use classic JSX runtime for better compatibility with React 19
      jsxRuntime: 'classic'
    })
  ],
  build: {
    // Ignore source map warnings for external dependencies
    rollupOptions: {
      onwarn(warning, warn) {
        // Suppress source map warnings for Speech SDK
        if (warning.code === 'SOURCEMAP_ERROR' && warning.message.includes('microsoft-cognitiveservices-speech-sdk')) {
          return;
        }
        warn(warning);
      }
    },
    sourcemap: false // Disable source maps to avoid warnings in development
  },
  define: {
    // Ensure global is available for compatibility
    global: 'globalThis',
  }
})
