"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const storage = require("./storage.js");

test("storage persists and restores a sender when available", () => {
  const values = new Map();
  const host = {
    localStorage: {
      getItem: (key) => values.get(key) ?? null,
      setItem: (key, value) => values.set(key, value),
    },
  };
  assert.equal(storage.write(host, "sender", "Ada"), true);
  assert.equal(storage.read(host, "sender"), "Ada");
});

test("storage access denial is an optional capability", () => {
  const denied = {};
  Object.defineProperty(denied, "localStorage", {
    get() {
      throw new DOMException("denied", "SecurityError");
    },
  });
  assert.equal(storage.read(denied, "sender"), "");
  assert.equal(storage.write(denied, "sender", "Ada"), false);
});

test("storage operation failures do not escape", () => {
  const unavailable = {
    localStorage: {
      getItem() {
        throw new Error("unavailable");
      },
      setItem() {
        throw new Error("quota exceeded");
      },
    },
  };
  assert.equal(storage.read(unavailable, "sender"), "");
  assert.equal(storage.write(unavailable, "sender", "Ada"), false);
});
