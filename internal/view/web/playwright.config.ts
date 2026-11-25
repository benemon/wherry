import { defineConfig } from '@playwright/test';

// The smoke runs the committed bundle against smoke/server.mjs, a fixture server with the Go server's policy and
// routes; make ui must have built ../static first.
const port = 4317;

export default defineConfig({
  testDir: 'smoke',
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:' + port,
    browserName: 'chromium',
    viewport: { width: 1280, height: 900 },
  },
  webServer: {
    command: 'node smoke/server.mjs ' + port,
    url: 'http://127.0.0.1:' + port + '/',
    reuseExistingServer: false,
    timeout: 20000,
  },
});
