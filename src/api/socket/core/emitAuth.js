"use strict";

module.exports = function createEmitAuth({ logger }) {
  const clearResource = (ctx, key, isArray = false) => {
    try {
      if (!ctx[key]) return;
      if (isArray && Array.isArray(ctx[key])) {
        ctx[key].forEach(clearInterval);
        ctx[key] = [];
      } else {
        clearInterval(ctx[key]);
        clearTimeout(ctx[key]);
        ctx[key] = null;
      }
    } catch (_) { /* Ignore cleanup errors */ }
  };

  return function emitAuth(ctx, api, globalCallback, reason, detail) {
    ctx.loggedIn = false;
    ctx._ending = true;
    ctx._cycling = false;

    ["_autoCycleTimer", "_reconnectTimer", "_rTimeout"].forEach(key => clearResource(ctx, key));

    ["_userInfoIntervals", "_autoSaveInterval"].forEach(key => clearResource(ctx, key, true));

    if (ctx.mqttClient) {
      try {
        ctx.mqttClient.removeAllListeners();
        if (ctx.mqttClient.connected) {
          ctx.mqttClient.end(true);
        }
      } catch (e) {
        logger(`Error cleaning up MQTT: ${e.message}`, "warn");
      } finally {
        ctx.mqttClient = undefined;
      }
    }

    if (ctx.tasks instanceof Map) ctx.tasks.clear();
    if (ctx._scheduler?.destroy) {
      try {
        ctx._scheduler.destroy();
      } catch (_) { }
      ctx._scheduler = undefined;
    }

    const msg = detail || reason;
    logger(`auth change -> ${reason}: ${msg}`, "error");

    if (typeof globalCallback === "function") {
      try {
        globalCallback({
          type: "account_inactive",
          reason,
          error: msg,
          timestamp: Date.now()
        }, null);
      } catch (cbErr) {
        logger(`emitAuth callback error: ${cbErr?.message || String(cbErr)}`, "error");
      }
    }
  };
};