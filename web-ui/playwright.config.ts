import { defineConfig, devices } from "@playwright/test";

const BACKEND_PORT = 8787;
const FRONTEND_PORT = 5173;

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL: `http://127.0.0.1:${FRONTEND_PORT}`,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: [
    {
      command: `python -m uvicorn datasus_etl.web.server:create_app --factory --port ${BACKEND_PORT}`,
      port: BACKEND_PORT,
      cwd: "..",
      reuseExistingServer: !process.env.CI,
      env: {
        DATASUS_DATA_DIR: "./web-ui/tests/.tmp-data",
      },
      timeout: 60_000,
    },
    {
      command: "bun run dev",
      port: FRONTEND_PORT,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
    },
  ],
});
