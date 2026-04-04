"use strict";

const fs = require("fs");
const { Readable } = require("stream");
const axios = require("axios");
const { isReadableStream } = require("../../utils/constants");

const parseFB = (b) => { 
    try { 
        return JSON.parse(String(b).replace(/^for\s*\(\s*;\s*;\s*\);\s*/, "")); 
    } catch { 
        return null; 
    } 
};

const getStreamSource = async (input) => {
    let stream;
    if (Buffer.isBuffer(input)) {
        stream = Readable.from(input);
    } else if (typeof input === "string") {
        if (/^https?:\/\//.test(input)) {
            const res = await axios.get(input, { responseType: 'stream', timeout: 300000 });
            stream = res.data;
        } else if (fs.existsSync(input)) {
            stream = fs.createReadStream(input);
        }
    } else if (isReadableStream(input)) {
        stream = input;
    }

    if (!stream) throw new Error("Invalid input for Stream Source");
    return { data: stream };
};

module.exports = (defaultFuncs, api, ctx) => {
    return async (inputs, opts = {}, cb) => {
        const callback = typeof opts === "function" ? opts : (typeof cb === "function" ? cb : () => {});
        const results = [];
        const inputList = [].concat(inputs);
        const mercuryUploadService = async (input) => {
            const source = await getStreamSource(input);
            const res = await defaultFuncs.postFormData("https://www.facebook.com/ajax/mercury/upload.php", ctx.jar, { upload_1024: source.data });
            const meta = Object.values(parseFB(res?.body)?.payload?.metadata || {})[0];
            if (!meta) throw new Error("Mercury upload failed to return valid metadata");
            return { 
                type: (meta.filetype || "file").replace("\\/", "/"), 
                mediaId: meta.fbid || meta.image_id || meta.video_id || meta.audio_id || meta.media_id 
            };
        };
        try {
            for (const input of inputList) {
                try {
                  const result = await mercuryUploadService(input);
                  results.push(result);
                } catch (e) {
                  throw e;
                }
            }
            callback(null, results);
            return results;
        } catch (e) { 
            callback(e); 
            throw e; 
        }
    };
};