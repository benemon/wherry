// A fixture server for the browser smoke: the committed bundle under the Go server's routes, headers and policy,
// with the fixtures of internal/view/web/fixtures as the API. It is not the Go server; the Go tests cover that one.
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const staticDir = join(here, '..', '..', 'static');
const fixtures = join(here, '..', 'fixtures');
const port = Number(process.argv[2] || 4317);

// The same policy the Go server sends, with the nonce on the page alone.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'";
const types = { '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.woff2': 'font/woff2', '.svg': 'image/svg+xml', '.png': 'image/png' };

export const token = 'a'.repeat(64);
const session = 'smoke-session';
const fixture = (name) => readFileSync(join(fixtures, name));

const workingRun = 'run-b04ce912bceb7864b872adaaa9c74f9c';
const failedRun = 'run-49f960713214457a3e21d20ece496b85';

const fail = (res, status, text) => {
  res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(text + '\n');
};

const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Content-Security-Policy', csp);
  res.setHeader('X-Content-Type-Options', 'nosniff');
  if (req.method === 'GET' && url.pathname === '/') {
    const nonce = randomBytes(16).toString('base64');
    res.setHeader('Content-Security-Policy', csp.replace("style-src 'self'", "style-src 'self' 'nonce-" + nonce + "'"));
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(readFileSync(join(staticDir, 'index.html'), 'utf8').replace('__CSP_NONCE__', nonce));
    return;
  }
  const asset = /^\/assets\/([^/]+)$/.exec(url.pathname);
  if (req.method === 'GET' && asset) {
    try {
      const body = readFileSync(join(staticDir, 'assets', asset[1]));
      res.setHeader('Content-Type', types[extname(asset[1])] || 'application/octet-stream');
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
      res.end(body);
    } catch {
      fail(res, 404, 'not found');
    }
    return;
  }
  if (req.method === 'POST' && url.pathname === '/session') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      if (new URLSearchParams(body).get('token') !== token) {
        fail(res, 403, 'forbidden');
        return;
      }
      res.setHeader('Set-Cookie', 'wherry-view-' + port + '=' + session + '; Path=/; HttpOnly; SameSite=Strict');
      res.writeHead(204);
      res.end();
    });
    return;
  }
  if (!(req.headers.cookie || '').includes('wherry-view-' + port + '=' + session)) {
    fail(res, 403, 'forbidden');
    return;
  }
  const json = (name) => {
    res.setHeader('Content-Type', 'application/json');
    res.end(fixture(name));
  };
  if (url.pathname === '/api/model') {
    return json('model.json');
  }
  if (url.pathname === '/api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write('event: model\ndata: {"version":7,"now":"2026-11-24T17:51:00Z","rebuilt":true,"runs":7}\n\n');
    const keepAlive = setInterval(() => res.write(': keep-alive\n\n'), 5000);
    req.on('close', () => clearInterval(keepAlive));
    return;
  }
  const run = /^\/api\/runs\/(run-[0-9a-f]{32})(\/trace|\/transcript)?$/.exec(url.pathname);
  if (run) {
    const [, id, part] = run;
    if (id !== workingRun && id !== failedRun) {
      return fail(res, 404, 'not found');
    }
    if (part === '/trace') {
      return json(id === failedRun ? 'trace.json' : 'trace-empty.json');
    }
    if (part === '/transcript') {
      return json(id === failedRun ? 'transcript-tail.json' : 'transcript-caps.json');
    }
    return json(id === failedRun ? 'run-failed.json' : 'run-working.json');
  }
  fail(res, 404, 'not found');
});

server.listen(port, '127.0.0.1', () => {
  console.log('smoke server on http://127.0.0.1:' + port + '/#t=' + token);
});
