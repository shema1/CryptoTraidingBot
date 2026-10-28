import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// UI слухає лише 127.0.0.1 (ТЗ §14).
export default defineConfig({
  plugins: [react()],
  server: { host: "127.0.0.1", port: 5173, strictPort: true },
  preview: { host: "127.0.0.1", port: 4173, strictPort: true },
});
