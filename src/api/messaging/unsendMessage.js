"use strict";

const { generateOfflineThreadingID } = require("../../utils/format");
const log = require("../../../func/logAdapter");

module.exports = (defaultFuncs, api, ctx) => {
  return async function unsendMessage(messageID, threadID, callback) {
    let resolve, reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });

    const done = (err, data) => {
      if (err) {
        log.error("unsendMessage", err);
        callback?.(err);
        return reject(err);
      }
      callback?.(null, data);
      resolve(data);
    };

    if (!ctx.mqttClient) return done(new Error("Not connected to MQTT"));

    const reqID = ++ctx.wsReqNumber;
    const content = {
      app_id: "2220391788200892",
      payload: JSON.stringify({
        tasks: [{
          label: "33",
          payload: JSON.stringify({ message_id: messageID, thread_key: threadID, sync_group: 1 }),
          queue_name: "unsend_message",
          task_id: ++ctx.wsTaskNumber,
          failure_count: null,
        }],
        epoch_id: parseInt(generateOfflineThreadingID()),
        version_id: "25393437286970779",
      }),
      request_id: reqID,
      type: 3,
    };

    const handleRes = (topic, message) => {
      if (topic !== "/ls_resp") return;
      try {
        const jsonMsg = JSON.parse(message.toString());
        if (jsonMsg.request_id !== reqID) return;

        ctx.mqttClient.removeListener("message", handleRes);
        const payload = JSON.parse(jsonMsg.payload);
        const step = payload.step?.[1]?.[2]?.[2]?.[1];
        
        const result = (step?.[2] && step?.[4]) 
          ? { body: step[4], messageID: step[2] } 
          : { success: true };

        done(null, result);
      } catch (e) {
        done(null, { success: true });
      }
    };

    ctx.mqttClient.on("message", handleRes);
    
    try {
      ctx.mqttClient.publish("/ls_req", JSON.stringify(content), { qos: 1, retain: false });
    } catch (err) {
      ctx.mqttClient.removeListener("message", handleRes);
      done(err);
    }

    return promise;
  };
};