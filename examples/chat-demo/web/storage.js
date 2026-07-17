"use strict";

((root, factory) => {
  const storage = factory();
  if (typeof module === "object" && module.exports) module.exports = storage;
  if (root) root.ChatDemoStorage = storage;
})(typeof globalThis === "undefined" ? null : globalThis, () => {
  function read(host, key) {
    try {
      const value = host.localStorage.getItem(key);
      return typeof value === "string" ? value : "";
    } catch {
      return "";
    }
  }

  function write(host, key, value) {
    try {
      host.localStorage.setItem(key, value);
      return true;
    } catch {
      return false;
    }
  }

  return Object.freeze({ read, write });
});
