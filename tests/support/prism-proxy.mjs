// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// Same proxy the TypeScript SDK contract suite uses (archastro-js,
// packages/sdk/__tests__/contract/prism-proxy.mjs). Keep the two in step.

// Runs Prism behind a proxy that rewrites `name[]=value` query params to
// `name=value`. The SDKs send arrays in the bracket form Platform expects,
// but Prism looks params up by their exact name.
//
// Usage: node prism-proxy.mjs <port> <prism command...>
// The Prism command gets `--port <free port>` appended. The proxy listens on
// <port> once Prism answers, and exits when Prism exits, with 128 + the
// signal number if a signal stopped Prism.

import { spawn } from "node:child_process";
import http from "node:http";
import net from "node:net";
import { constants } from "node:os";

const [port, command, ...args] = process.argv.slice(2);
const prismPort = await freePort();
const prism = spawn(command, [...args, "--port", String(prismPort)], {
  stdio: "inherit",
});
prism.on("exit", (code, signal) =>
  process.exit(code ?? 128 + (constants.signals[signal] ?? 0))
);
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.on(signal, () => prism.kill(signal));
}

await waitForPrism();

http
  .createServer((req, res) => {
    const upstream = http.request(
      {
        host: "127.0.0.1",
        port: prismPort,
        method: req.method,
        path: toPrismPath(req.url ?? "/"),
        headers: req.headers,
      },
      (prismRes) => {
        res.writeHead(prismRes.statusCode ?? 502, prismRes.headers);
        prismRes.pipe(res);
      }
    );
    upstream.on("error", (error) => {
      res.writeHead(502);
      res.end(String(error));
    });
    req.pipe(upstream);
  })
  .listen(Number(port), "127.0.0.1");

function toPrismPath(url) {
  const queryStart = url.indexOf("?");
  if (queryStart < 0) return url;
  const query = url
    .slice(queryStart)
    .replace(/([?&][^=&]*?)(?:\[\]|%5B%5D)(?=[=&]|$)/gi, "$1");
  return url.slice(0, queryStart) + query;
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

async function waitForPrism() {
  for (;;) {
    const ready = await new Promise((resolve) => {
      http
        .get({ host: "127.0.0.1", port: prismPort, path: "/" }, (res) => {
          res.resume();
          resolve(true);
        })
        .on("error", () => resolve(false));
    });
    if (ready) return;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}
