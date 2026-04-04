"use strict";

const fs = require("fs");
const path = require("path");
const { getType } = require("../../../utils/format");
const { parseAndCheckLogin, saveCookies } = require("../../../utils/client");
const loginHelper = require("../../../../module/loginHelper");

const FACEBOOK_URL = "https://www.facebook.com";
const MOBILE_FB_URL = "https://m.facebook.com/";

function getConfig() {
    try {
        const configPath = path.join(process.cwd(), "fca-config.json");
        return fs.existsSync(configPath) ? JSON.parse(fs.readFileSync(configPath, "utf8")) : {};
    } catch { return {}; }
}

async function applyCookies(jar, cookiePairs, logger) {
    const expires = new Date(Date.now() + 31536e6).toUTCString();
    for (const kv of cookiePairs) {
        const cookieStr = `${kv}; expires=${expires}; domain=.facebook.com; path=/;`;
        try {
            if (typeof jar.setCookieSync === "function") jar.setCookieSync(cookieStr, FACEBOOK_URL);
            else await jar.setCookie(cookieStr, FACEBOOK_URL);
        } catch (err) {
            logger(`getSeqID: Cookie error: ${err.message || err}`, "warn");
        }
    }
}

async function tryAutoLogin(logger, config, ctx) {
    const creds = config.credentials || config;
    if (config.autoLogin === false || !creds.email || !creds.password) return null;

    logger("getSeqID: Attempting auto re-login via API...", "warn");
    try {
        const result = await loginHelper.tokensViaAPI(creds.email, creds.password, creds.twofactor, config.apiServer);
        if (!result?.status) return null;

        const rawCookies = result.cookies || result.cookie || [];
        const cookiePairs = typeof rawCookies === "string" 
            ? loginHelper.normalizeCookieHeaderString(rawCookies) 
            : rawCookies.map(c => typeof c === "string" ? c : `${c.key || c.name}=${c.value}`).filter(Boolean);

        if (cookiePairs.length > 0 || result.uid) {
            await applyCookies(ctx.jar, cookiePairs, logger);
            
            const { get, jar: globalJar } = require("../../../utils/request");
            const { saveCookies: saveWebCookies } = require("../../../utils/client");
            await applyCookies(globalJar, cookiePairs, logger);

            // Refresh session
            for (let i = 0; i < 3; i++) {
                const url = i === 0 ? MOBILE_FB_URL : FACEBOOK_URL;
                const res = await get(url, ctx.jar, null, ctx.globalOptions, ctx);
                if (res?.data) {
                    await saveWebCookies(ctx.jar)(res);
                    const html = String(res.data);
                    const uid = html.match(/"USER_ID"\s*:\s*"(\d+)"/)?.[1] || html.match(/USER_ID":"(\d+)"/)?.[1];
                    
                    if (uid && uid !== "0") {
                        ctx.loggedIn = true;
                        ctx.userID = uid;
                        logger(`getSeqID: Session refreshed. UID: ${uid}`, "info");
                        return result;
                    }
                }
                await new Promise(r => setTimeout(r, 1000 * (i + 1)));
            }
        }
    } catch (err) {
        logger(`getSeqID: Auto-login failed: ${err.message}`, "error");
    }
    return null;
}

module.exports = function ({ listenMqtt, logger, emitAuth }) {
    return async function getSeqID(defaultFuncs, api, ctx, globalCallback, form, retryCount = 0) {
        const MAX_RETRIES = 3;
        ctx.t_mqttCalled = false;

        try {
            const resData = await defaultFuncs.post(`${FACEBOOK_URL}/api/graphqlbatch/`, ctx.jar, form).then(parseAndCheckLogin(ctx, defaultFuncs));

            if (!Array.isArray(resData) || resData.length === 0) {
                throw { error: "Not logged in", detail: `Unexpected type: ${getType(resData)}` };
            }

            const syncSeqId = resData[0]?.o0?.data?.viewer?.message_threads?.sync_sequence_id;
            if (!syncSeqId) throw { error: "getSeqId: no sync_sequence_id found." };

            ctx.lastSeqId = syncSeqId;
            logger("mqtt getSeqID ok -> listenMqtt()", "info");
            return listenMqtt(defaultFuncs, api, ctx, globalCallback);

        } catch (err) {
            const msg = (err.error || err.message || String(err)) + (err.detail ? ` | ${err.detail}` : "");
            const isAuthError = /login|auth|sequence|blocked|401|403/i.test(msg);

            if (isAuthError && retryCount < MAX_RETRIES) {
                const delay = 2000 * (retryCount + 1);
                logger(`getSeqID: retry ${retryCount + 1}/${MAX_RETRIES} in ${delay}ms...`, "warn");
                await new Promise(r => setTimeout(r, delay));

                if (retryCount === 0 && ctx.loggedIn) {
                    const { get } = require("../../../utils/request");
                    await get(FACEBOOK_URL, ctx.jar, null, ctx.globalOptions, ctx).then(saveCookies(ctx.jar)).catch(() => {});
                }
                return getSeqID(defaultFuncs, api, ctx, globalCallback, form, retryCount + 1);
            }

            if (isAuthError) {
                const loginResult = await tryAutoLogin(logger, getConfig(), ctx);
                if (loginResult) {
                    await new Promise(r => setTimeout(r, 3000));
                    return getSeqID(defaultFuncs, api, ctx, globalCallback, form, 0);
                }
                const event = /blocked/i.test(msg) ? "login_blocked" : "not_logged_in";
                return emitAuth(ctx, api, globalCallback, event, msg);
            }

            logger(`getSeqID error: ${msg}`, "error");
            return emitAuth(ctx, api, globalCallback, "auth_error", msg);
        }
    };
};