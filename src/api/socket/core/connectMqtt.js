"use strict";

const { formatID } = require("../../../utils/format");

const DEFAULT_RECONNECT_DELAY = 2000;
const T_MS_WAIT_TIMEOUT = 5000;

module.exports = function createListenMqtt(deps) {
  const { 
    WebSocket, mqtt, HttpsProxyAgent, buildStream, buildProxy,
    topics, parseDelta, getTaskResponseData, logger, emitAuth 
  } = deps;

  return function listenMqtt(defaultFuncs, api, ctx, globalCallback) {
    const chatOn = !!ctx.globalOptions.online;
    const sessionID = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER) + 1;
    const reconnectDelay = ctx._mqttOpt?.reconnectDelayMs || DEFAULT_RECONNECT_DELAY;

    const safeDisconnect = () => {
      try {
        if (ctx.mqttClient?.connected) ctx.mqttClient.end(true);
      } catch (_) {}
    };

    const scheduleReconnect = (delay = reconnectDelay) => {
      if (ctx._reconnectTimer || ctx._ending) return;
      
      logger(`mqtt will reconnect in ${delay}ms`, "warn");
      ctx._reconnectTimer = setTimeout(() => {
        ctx._reconnectTimer = null;
        if (!ctx._ending) listenMqtt(defaultFuncs, api, ctx, globalCallback);
      }, delay);
    };

    const host = ctx.mqttEndpoint 
      ? `${ctx.mqttEndpoint}&sid=${sessionID}&cid=${ctx.clientId}`
      : `wss://edge-chat.facebook.com/chat?${ctx.region ? `region=${ctx.region.toLowerCase()}&` : ""}sid=${sessionID}&cid=${ctx.clientId}`;

    const username = {
      u: ctx.userID, s: sessionID, chat_on: chatOn, fg: false, d: ctx.clientId,
      ct: "websocket", aid: 219994525426954, aids: null, mqtt_sid: "",
      cp: 3, ecp: 10, st: [], pm: [], dc: "", no_auto_fg: true, gas: null, pack: [], p: null, php_override: ""
    };

    const options = {
      clientId: "mqttwsclient",
      protocolId: "MQIsdp",
      protocolVersion: 3,
      username: JSON.stringify(username),
      clean: true,
      keepalive: 30,
      reschedulePings: true,
      reconnectPeriod: 0,
      connectTimeout: 5000,
      wsOptions: {
        headers: {
          Cookie: api.getCookies(),
          Origin: "https://www.facebook.com",
          "User-Agent": ctx.globalOptions.userAgent || "Mozilla/5.0",
          Referer: "https://www.facebook.com/",
          Host: "edge-chat.facebook.com",
        },
        origin: "https://www.facebook.com",
        protocolVersion: 13,
        binaryType: "arraybuffer",
        agent: ctx.globalOptions.proxy ? new HttpsProxyAgent(ctx.globalOptions.proxy) : undefined
      }
    };

    ctx.mqttClient = new mqtt.Client(() => buildStream(options, new WebSocket(host, options.wsOptions), buildProxy()), options);
    const client = ctx.mqttClient;

    client.on("error", (err) => {
      const msg = err?.message || String(err || "");
      if (ctx._ending || ctx._cycling) return;

      if (/Not logged in|blocked|401|403/i.test(msg)) {
        safeDisconnect();
        return emitAuth(ctx, api, globalCallback, /blocked/i.test(msg) ? "login_blocked" : "not_logged_in", msg);
      }

      logger(`mqtt error: ${msg}`, "error");
      safeDisconnect();

      if (ctx.globalOptions.autoReconnect) scheduleReconnect();
      else globalCallback({ type: "stop_listen", error: msg }, null);
    });

    client.on("connect", () => {
      if (!process.env.OnStatus) {
        logger("fca-unofficial - MQTT Connected", "info");
        process.env.OnStatus = "true";
      }
      ctx._cycling = false;
      topics.forEach(t => client.subscribe(t));

      const syncTopic = ctx.syncToken ? "/messenger_sync_get_diffs" : "/messenger_sync_create_queue";
      const syncPayload = {
        sync_api_version: 11, max_deltas_able_to_process: 100, delta_batch_size: 500,
        encoding: "JSON", entity_fbid: ctx.userID, initial_titan_sequence_id: ctx.lastSeqId,
        ...(ctx.syncToken && { last_seq_id: ctx.lastSeqId, sync_token: ctx.syncToken })
      };

      client.publish(syncTopic, JSON.stringify(syncPayload), { qos: 1 });
      client.publish("/foreground_state", JSON.stringify({ foreground: chatOn }), { qos: 1 });

      let rTimeout = setTimeout(() => {
        if (ctx._ending) return;
        logger("mqtt t_ms timeout, cycling...", "warn");
        safeDisconnect();
        scheduleReconnect();
      }, T_MS_WAIT_TIMEOUT);

      ctx._rTimeout = rTimeout;
      ctx.tmsWait = () => {
        clearTimeout(rTimeout);
        delete ctx._rTimeout;
        if (ctx.globalOptions.emitReady) globalCallback({ type: "ready", error: null });
        delete ctx.tmsWait;
      };
    });

    client.on("message", (topic, rawMessage) => {
      if (ctx._ending) return;
      
      try {
        const message = JSON.parse(Buffer.isBuffer(rawMessage) ? rawMessage.toString() : rawMessage);

        switch (topic) {
          case "/t_ms":
            if (ctx.tmsWait) ctx.tmsWait();
            if (message.firstDeltaSeqId && message.syncToken) {
              ctx.lastSeqId = message.firstDeltaSeqId;
              ctx.syncToken = message.syncToken;
            }
            if (message.lastIssuedSeqId) ctx.lastSeqId = parseInt(message.lastIssuedSeqId);
            (message.deltas || []).forEach(delta => parseDelta(defaultFuncs, api, ctx, globalCallback, { delta }));
            break;

          case "/thread_typing":
          case "/orca_typing_notifications":
            globalCallback(null, {
              type: "typ",
              isTyping: !!message.state,
              from: message.sender_fbid.toString(),
              threadID: formatID((message.thread || message.sender_fbid).toString())
            });
            break;

          case "/orca_presence":
            if (!ctx.globalOptions.updatePresence) {
              (message.list || []).forEach(data => {
                globalCallback(null, { type: "presence", userID: String(data.u), timestamp: data.l * 1000, statuses: data.p });
              });
            }
            break;

          case "/ls_resp":
            const parsedPayload = JSON.parse(message.payload);
            const task = ctx.tasks?.get(message.request_id);
            if (task) {
              const resp = getTaskResponseData(task.type, parsedPayload);
              resp ? task.callback(null, { type: task.type, reqID: message.request_id, ...resp }) : task.callback("error", null);
            }
            break;

          default:
            if (message.type === "jewel_requests_add") {
              globalCallback(null, { type: "friend_request_received", actorFbId: message.from.toString(), timestamp: Date.now().toString() });
            }
        }
      } catch (ex) {
        logger(`mqtt message error [${topic}]: ${ex.message}`, "error");
      }
    });
    client.on("close", () => !ctx._ending && !ctx._cycling && logger("mqtt connection closed", "warn"));
  };
};