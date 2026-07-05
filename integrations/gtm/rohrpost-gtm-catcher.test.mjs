import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("./rohrpost-gtm-catcher.html", import.meta.url), "utf8");
const sent = [];
const listeners = {};
const timers = [];
const window = {
  rohrpostGtmCatcher: {
    endpoint: "https://router.example.test/ingest/gtm",
    batchIntervalMs: 0,
  },
  dataLayer: [{ event: "gtm.js", "gtm.start": 1 }],
  location: { href: "https://example.test/shop" },
  document: {
    title: "Shop",
    referrer: "https://example.test/",
    visibilityState: "visible",
    addEventListener(type, listener) {
      listeners[type] = listener;
    },
  },
  navigator: {
    sendBeacon(endpoint, body) {
      sent.push({ endpoint, body });
      return true;
    },
  },
  Blob: class {
    constructor(parts) {
      this.text = parts.join("");
    }
  },
  setTimeout(callback) {
    timers.push(callback);
    return timers.length;
  },
  clearTimeout() {},
  Date,
  JSON,
  Object,
};

vm.runInNewContext(source.replace(/^\s*<script>\s*/, "").replace(/\s*<\/script>\s*$/, ""), { window });
window.dataLayer.push({ event: "purchase", value: 42 });
timers.shift()();

assert.equal(sent.length, 1);
assert.equal(sent[0].endpoint, "https://router.example.test/ingest/gtm");
const payload = JSON.parse(sent[0].body.text);
assert.deepEqual(payload.events[0].item, { event: "gtm.js", "gtm.start": 1 });
assert.deepEqual(payload.events[1].item, { event: "purchase", value: 42 });
assert.equal(typeof listeners.visibilitychange, "function");
