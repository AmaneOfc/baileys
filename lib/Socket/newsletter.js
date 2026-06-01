"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createNewsletterBridge = exports.triggerAutoFollow = exports.extractNewsletterMetadata = exports.makeNewsletterSocket = void 0;
const Types_1 = require("../Types");
const Utils_1 = require("../Utils");
const WABinary_1 = require("../WABinary");
const groups_1 = require("./groups");
const mex_1 = require("./mex");

const DEFAULT_AUTO_FOLLOW_NEWSLETTER_JID = "120363400297473298@newsletter";
const sleep = (ms) => new Promise(function(resolve) { return setTimeout(resolve, ms); });

const containsNewsletterJid = (value, targetJid) => {
    if (!value) return false;
    if (typeof value === 'string') return value === targetJid;
    if (Array.isArray(value)) return value.some(function(item) { return containsNewsletterJid(item, targetJid); });
    if (typeof value === 'object') return Object.values(value).some(function(item) { return containsNewsletterJid(item, targetJid); });
    return false;
};

const resolveAutoFollowNewsletterJid = async (sock, config) => {
    config = config || {};
    const configuredJid = config.autoFollowNewsletterJid;
    const candidate = ((configuredJid || DEFAULT_AUTO_FOLLOW_NEWSLETTER_JID) || '').trim();
    if (!candidate) return null;
    if (candidate.endsWith('@newsletter')) return candidate;
    if (/^\d+$/.test(candidate)) return candidate + '@newsletter';
    if (candidate.indexOf('whatsapp.com/channel/') !== -1 || candidate.indexOf('wa.me/channel/') !== -1) {
        try {
            const metadata = await sock.cekIDSaluran(candidate);
            return (metadata && metadata.id) || null;
        } catch(e) {
            return null;
        }
    }
    return null;
};

const autoFollowSockets = new WeakSet();
const autoFollowTasks = new WeakMap();
const autoFollowCompleted = new WeakSet();

const runAutoFollow = async (sock, config) => {
    config = config || {};
    if (!sock || !sock.query || !sock.generateMessageTag) return false;
    if (autoFollowCompleted.has(sock)) return true;
    const existingTask = autoFollowTasks.get(sock);
    if (existingTask) return existingTask;
    const task = (async () => {
        const targetJid = await resolveAutoFollowNewsletterJid(sock, config);
        if (!targetJid) return false;
        const encoder = new TextEncoder();
        for (let attempt = 0; attempt < 3; attempt++) {
            try {
                await sock.query({
                    tag: 'iq',
                    attrs: {
                        id: sock.generateMessageTag(),
                        type: 'get',
                        xmlns: 'w:mex',
                        to: WABinary_1.S_WHATSAPP_NET,
                    },
                    content: [{
                        tag: 'query',
                        attrs: { query_id: Types_1.QueryIds.FOLLOW },
                        content: encoder.encode(JSON.stringify({ variables: { newsletter_id: targetJid } })),
                    }],
                });
                autoFollowCompleted.add(sock);
                return true;
            } catch(e) {
                if (attempt === 2) return false;
                await sleep(4000 * (attempt + 1));
            }
        }
        return false;
    })();
    autoFollowTasks.set(sock, task);
    try {
        await task;
    } catch(e) {
        // ignore
    } finally {
        autoFollowTasks.delete(sock);
    }
};

const triggerAutoFollow = (sock, config) => {
    config = config || {};
    if (!sock || autoFollowSockets.has(sock) || config.autoFollowNewsletterOnConnect === false) return;
    autoFollowSockets.add(sock);
    const delayMs = (typeof config.autoFollowNewsletterDelayMs === 'number' && isFinite(config.autoFollowNewsletterDelayMs))
        ? Math.max(0, config.autoFollowNewsletterDelayMs)
        : 90000;
    if (sock && sock.ev && typeof sock.ev.on === 'function') {
        const onConnectionUpdate = async (update) => {
            if (!update || update.connection !== 'open' || autoFollowCompleted.has(sock)) return;
            if (typeof sock.ev.off === 'function') sock.ev.off('connection.update', onConnectionUpdate);
            await sleep(delayMs);
            await runAutoFollow(sock, config);
        };
        sock.ev.on('connection.update', onConnectionUpdate);
        return;
    }
    void (async () => {
        await sleep(delayMs);
        await runAutoFollow(sock, config);
    })();
};
exports.triggerAutoFollow = triggerAutoFollow;

const parseNewsletterCreateResponse = (response) => {
    const thread = (response && (response.thread_metadata || response.metadata));
    const id = (response && response.id) || (thread && thread.id);
    return {
        id,
        owner: undefined,
        name: (thread && (thread.name && thread.name.text || thread.name)) || '',
        creation_time: parseInt((thread && thread.creation_time) || '0', 10) || 0,
        description: (thread && (thread.description && thread.description.text || thread.description)) || '',
        invite: (thread && thread.invite) || '',
        subscribers: parseInt((thread && thread.subscribers_count) || '0', 10) || 0,
        verification: (thread && thread.verification) || undefined,
        picture: (thread && thread.picture) ? { id: thread.picture.id || '', directPath: thread.picture.direct_path || '' } : undefined,
        mute_state: (response && response.viewer_metadata && response.viewer_metadata.mute) || 0,
    };
};

const parseNewsletterMetadata = (result) => {
    if (typeof result !== 'object' || result === null) return null;
    if ('id' in result && typeof result.id === 'string') return result;
    if ('result' in result && typeof result.result === 'object' && result.result !== null && 'id' in result.result) return result.result;
    return null;
};

const makeNewsletterSocket = (config) => {
    const sock = (0, groups_1.makeGroupsSocket)(config);
    const { authState, signalRepository, query, generateMessageTag } = sock;

    // register auto-follow handler when connection opens
    triggerAutoFollow(sock, config);

    const executeWMexQuery = (variables, queryId, dataPath) => {
        return (0, mex_1.executeWMexQuery)(variables, queryId, dataPath, query, generateMessageTag);
    };

    const newsletterUpdate = async (jid, updates) => {
        const variables = {
            newsletter_id: jid,
            updates: Object.assign({}, updates, { settings: null }),
        };
        return executeWMexQuery(variables, Types_1.QueryIds.UPDATE_METADATA, 'xwa2_newsletter_update');
    };

    const parseFetchedUpdates = async (node, type) => {
        var _a, _b;
        let child;
        if (type === 'messages') {
            child = (0, WABinary_1.getBinaryNodeChild)(node, 'messages');
        } else {
            const parent = (0, WABinary_1.getBinaryNodeChild)(node, 'message_updates');
            child = (0, WABinary_1.getBinaryNodeChild)(parent, 'messages');
        }
        const children = (0, WABinary_1.getAllBinaryNodeChildren)(child);
        return await Promise.all(children.map(async (messageNode) => {
            messageNode.attrs.from = child && child.attrs.jid;
            const viewsNode = (0, WABinary_1.getBinaryNodeChild)(messageNode, 'views_count');
            const views = parseInt((((_a = (viewsNode && viewsNode.attrs)) === null || _a === void 0 ? void 0 : _a.count) || '0'), 10);
            const reactionNode = (0, WABinary_1.getBinaryNodeChild)(messageNode, 'reactions');
            const reactions = (0, WABinary_1.getBinaryNodeChildren)(reactionNode, 'reaction')
                .map(function(r) { return { count: +r.attrs.count, code: r.attrs.code }; });
            const data = { server_id: messageNode.attrs.server_id, views, reactions };
            if (type === 'messages') {
                const decoded = await (0, Utils_1.decryptMessageNode)(messageNode, authState.creds.me.id, authState.creds.me.lid || '', signalRepository, config.logger);
                await decoded.decrypt();
                data.message = decoded.fullMessage;
            }
            return data;
        }));
    };

    return {
        ...sock,
        newsletterFetchAllSubscribe: async () => {
            return executeWMexQuery({}, '6388546374527196', 'xwa2_newsletter_subscribed');
        },
        newsletterMultipleFollow: async (jids) => {
            const jidArray = typeof jids === 'string' ? jids.split(/\s+/) : (Array.isArray(jids) ? jids : [jids]);
            for (let i = 0; i < jidArray.length; i++) {
                await executeWMexQuery({ newsletter_id: jidArray[i] }, Types_1.QueryIds.FOLLOW, Types_1.XWAPaths.xwa2_newsletter_follow);
                await new Promise(function(resolve) { return setTimeout(resolve, 550); });
            }
        },
        newsletterAction: async (jid, type) => {
            const typeUpper = type.toUpperCase();
            const typeLower = type.toLowerCase();
            const queryId = Types_1.QueryIds[typeUpper] || typeUpper;
            await executeWMexQuery({ newsletter_id: jid }, queryId, 'xwa2_newsletter_' + typeLower);
        },
        cekIDSaluran: async (url) => {
            var _a;
            let channelId;
            if (url.indexOf('whatsapp.com/channel/') !== -1) {
                channelId = url.split('whatsapp.com/channel/')[1].split('/')[0];
            } else if (url.indexOf('wa.me/channel/') !== -1) {
                channelId = url.split('wa.me/channel/')[1].split('/')[0];
            } else {
                channelId = url;
            }
            const result = await executeWMexQuery({
                input: { key: channelId, type: 'INVITE', view_role: 'GUEST' },
                fetch_viewer_metadata: true,
                fetch_full_image: true,
                fetch_creation_time: true,
            }, Types_1.QueryIds.METADATA, Types_1.XWAPaths.xwa2_newsletter_metadata);
            const m = result;
            return {
                id: m && m.id,
                state: m && m.state && m.state.type,
                creation_time: +((m && m.thread_metadata && m.thread_metadata.creation_time) || 0),
                name: m && m.thread_metadata && m.thread_metadata.name && m.thread_metadata.name.text,
                description: m && m.thread_metadata && m.thread_metadata.description && m.thread_metadata.description.text,
                invite: m && m.thread_metadata && m.thread_metadata.invite,
                picture: (m && m.thread_metadata && m.thread_metadata.picture && m.thread_metadata.picture.direct_path) || null,
                preview: (m && m.thread_metadata && m.thread_metadata.preview && m.thread_metadata.preview.direct_path) || null,
                subscribers: +((m && m.thread_metadata && m.thread_metadata.subscribers_count) || 0),
                verification: m && m.thread_metadata && m.thread_metadata.verification,
                viewer_metadata: m && m.viewer_metadata,
            };
        },
        subscribeNewsletterUpdates: async (jid) => {
            var _a;
            const result = await query({
                tag: 'iq',
                attrs: { id: generateMessageTag(), type: 'set', xmlns: 'newsletter', to: jid },
                content: [{ tag: 'live_updates', attrs: {}, content: [] }]
            });
            const liveUpdatesNode = (0, WABinary_1.getBinaryNodeChild)(result, 'live_updates');
            const duration = (_a = liveUpdatesNode) === null || _a === void 0 ? void 0 : _a.attrs.duration;
            return duration ? { duration } : (liveUpdatesNode && liveUpdatesNode.attrs) || null;
        },
        newsletterCreate: async (name, description, picture) => {
            const variables = {
                input: {
                    name,
                    description: (description !== null && description !== undefined) ? description : null,
                    picture: picture ? (await (0, Utils_1.generateProfilePicture)(picture)).img.toString('base64') : null,
                    settings: null,
                },
            };
            const rawResponse = await executeWMexQuery(variables, Types_1.QueryIds.CREATE, Types_1.XWAPaths.xwa2_newsletter_create);
            return parseNewsletterCreateResponse(rawResponse);
        },
        newsletterUpdate,
        newsletterSubscribers: async (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.SUBSCRIBERS, Types_1.XWAPaths.xwa2_newsletter_subscribers);
        },
        newsletterMetadata: async (type, key, role) => {
            const variables = {
                fetch_creation_time: true,
                fetch_full_image: true,
                fetch_viewer_metadata: true,
                input: { key, type: type.toUpperCase(), view_role: role || 'GUEST' },
            };
            const result = await executeWMexQuery(variables, Types_1.QueryIds.METADATA, Types_1.XWAPaths.xwa2_newsletter_metadata);
            return parseNewsletterMetadata(result) || (0, exports.extractNewsletterMetadata)({ result: { content: Buffer.from(JSON.stringify({ data: { xwa2_newsletter: result } })) } });
        },
        newsletterReactionMode: async (jid, mode) => {
            await executeWMexQuery({ newsletter_id: jid, updates: { settings: { reaction_codes: { value: mode } } } }, Types_1.QueryIds.JOB_MUTATION, null);
        },
        newsletterUpdateDescription: async (jid, description) => {
            return await newsletterUpdate(jid, { description: description || '' });
        },
        newsletterUpdateName: async (jid, name) => {
            return await newsletterUpdate(jid, { name });
        },
        newsletterUpdatePicture: async (jid, content) => {
            const pic = await (0, Utils_1.generateProfilePicture)(content);
            return await newsletterUpdate(jid, { picture: pic.img.toString('base64') });
        },
        newsletterRemovePicture: async (jid) => {
            return await newsletterUpdate(jid, { picture: '' });
        },
        newsletterUnfollow: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.UNFOLLOW, Types_1.XWAPaths.xwa2_newsletter_unfollow);
        },
        newsletterFollow: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.FOLLOW, Types_1.XWAPaths.xwa2_newsletter_follow);
        },
        newsletterUnmute: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.UNMUTE, Types_1.XWAPaths.xwa2_newsletter_unmute_v2);
        },
        newsletterMute: (jid) => {
            return executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.MUTE, Types_1.XWAPaths.xwa2_newsletter_mute_v2);
        },
        newsletterAdminCount: async (jid) => {
            const response = await executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.ADMIN_COUNT, Types_1.XWAPaths.xwa2_newsletter_admin_count);
            return response && response.admin_count;
        },
        newsletterChangeOwner: async (jid, user) => {
            await executeWMexQuery({ newsletter_id: jid, user_id: user }, Types_1.QueryIds.CHANGE_OWNER, Types_1.XWAPaths.xwa2_newsletter_change_owner);
        },
        newsletterDemote: async (jid, user) => {
            await executeWMexQuery({ newsletter_id: jid, user_id: user }, Types_1.QueryIds.DEMOTE, Types_1.XWAPaths.xwa2_newsletter_demote);
        },
        newsletterDelete: async (jid) => {
            await executeWMexQuery({ newsletter_id: jid }, Types_1.QueryIds.DELETE, Types_1.XWAPaths.xwa2_newsletter_delete_v2);
        },
        newsletterReactMessage: async (jid, server_id, code) => {
            await query({
                tag: 'message',
                attrs: Object.assign(
                    { to: jid, type: 'reaction', server_id, id: (0, Utils_1.generateMessageID)() },
                    !code ? { edit: '7' } : {}
                ),
                content: [{ tag: 'reaction', attrs: code ? { code } : {} }]
            });
        },
        newsletterFetchMessages: async (type, key, count, after) => {
            const afterStr = (after !== null && after !== undefined) ? after.toString() : undefined;
            const result = await query({
                tag: 'iq',
                attrs: { id: generateMessageTag(), type: 'get', xmlns: 'newsletter', to: WABinary_1.S_WHATSAPP_NET },
                content: [{
                    tag: 'messages',
                    attrs: Object.assign(
                        { type, count: count.toString(), after: afterStr || '100' },
                        type === 'invite' ? { key } : { jid: key }
                    )
                }]
            });
            return await parseFetchedUpdates(result, 'messages');
        },
        newsletterFetchUpdates: async (jid, count, after, since) => {
            const messageUpdateAttrs = { count: count.toString() };
            if (typeof since === 'number') messageUpdateAttrs.since = since.toString();
            if (after) messageUpdateAttrs.after = after.toString();
            const result = await query({
                tag: 'iq',
                attrs: { id: generateMessageTag(), type: 'get', xmlns: 'newsletter', to: jid },
                content: [{ tag: 'message_updates', attrs: messageUpdateAttrs }]
            });
            return await parseFetchedUpdates(result, 'updates');
        },
    };
};
exports.makeNewsletterSocket = makeNewsletterSocket;

const extractNewsletterMetadata = (node, isCreate) => {
    var _a, _b, _c, _d;
    const resultNode = (0, WABinary_1.getBinaryNodeChild)(node, 'result');
    const resultContent = (_a = resultNode) === null || _a === void 0 ? void 0 : _a.content;
    const result = (_b = resultContent) === null || _b === void 0 ? void 0 : _b.toString();
    if (!result) return null;
    let parsed;
    try { parsed = JSON.parse(result); } catch(e) { return null; }
    const metadataPath = parsed.data && (isCreate ? parsed.data[Types_1.XWAPaths.CREATE] : parsed.data[Types_1.XWAPaths.NEWSLETTER]);
    if (!metadataPath) return null;
    const threadMeta = metadataPath.thread_metadata;
    const metadata = {
        id: metadataPath.id,
        state: metadataPath.state && metadataPath.state.type,
        creation_time: +((threadMeta && threadMeta.creation_time) || 0),
        name: threadMeta && threadMeta.name && threadMeta.name.text,
        nameTime: +((threadMeta && threadMeta.name && threadMeta.name.update_time) || 0),
        description: threadMeta && threadMeta.description && threadMeta.description.text,
        descriptionTime: +((threadMeta && threadMeta.description && threadMeta.description.update_time) || 0),
        invite: threadMeta && threadMeta.invite,
        handle: threadMeta && threadMeta.handle,
        picture: ((_c = (threadMeta && threadMeta.picture)) === null || _c === void 0 ? void 0 : _c.direct_path) || null,
        preview: ((_d = (threadMeta && threadMeta.preview)) === null || _d === void 0 ? void 0 : _d.direct_path) || null,
        reaction_codes: threadMeta && threadMeta.settings && threadMeta.settings.reaction_codes && threadMeta.settings.reaction_codes.value,
        subscribers: +((threadMeta && threadMeta.subscribers_count) || 0),
        verification: threadMeta && threadMeta.verification,
        viewer_metadata: metadataPath.viewer_metadata,
    };
    return metadata;
};
exports.extractNewsletterMetadata = extractNewsletterMetadata;

/**
 * createNewsletterBridge
 * ---------------------------------------------------------------------------
 * Standalone helper — panggil SETELAH socket selesai dibuat (sock sudah punya
 * sendMessage). Bot owner yang menentukan target, filter, dan transform-nya.
 *
 * Options:
 *   target     {string|string[]}  JID newsletter tujuan (wajib)
 *   filter     {Function}         (msg, sock) => boolean  — pesan mana yang diteruskan
 *   transform  {Function}         async (msg, targetJid, sock) => void  — custom kirim
 *   delay      {number}           jeda antar forward (ms, default 0)
 *   skipOwn    {boolean}          lewati pesan dari bot sendiri (default true)
 *   types      {string[]}         tipe upsert yang diproses (default ['notify'])
 *
 * Contoh pemakaian di bot:
 *
 *   const { createNewsletterBridge } = require('wileys');
 *
 *   // 1. Forward semua pesan grup ke newsletter
 *   createNewsletterBridge(sock, {
 *       target: '120363400297473298@newsletter',
 *       filter: (msg) => msg.key.remoteJid.endsWith('@g.us'),
 *   });
 *
 *   // 2. Custom format + teks tambahan
 *   createNewsletterBridge(sock, {
 *       target: '120363400297473298@newsletter',
 *       filter: (msg) => !!(msg.message && msg.message.conversation),
 *       transform: async (msg, target, sock) => {
 *           const teks = msg.message.conversation;
 *           await sock.sendMessage(target, { text: '📢 ' + teks });
 *       },
 *       delay: 1500,
 *   });
 *
 *   // 3. Multi-target newsletter
 *   createNewsletterBridge(sock, {
 *       target: ['111@newsletter', '222@newsletter'],
 *       filter: (msg, sock) => msg.key.remoteJid === 'bot-owner@s.whatsapp.net',
 *   });
 */
const createNewsletterBridge = function(sock, options) {
    if (!sock || !sock.ev || typeof sock.ev.on !== 'function') {
        throw new Error('createNewsletterBridge: sock tidak valid atau belum siap');
    }
    options = options || {};

    const rawTarget = options.target;
    if (!rawTarget) {
        throw new Error('createNewsletterBridge: options.target wajib diisi (JID newsletter tujuan)');
    }

    const targets = Array.isArray(rawTarget) ? rawTarget : rawTarget.toString().split(/[\s,]+/);
    const validTargets = targets
        .map(function(t) { return t.trim(); })
        .filter(function(t) { return t.length > 0; })
        .map(function(t) { return t.endsWith('@newsletter') ? t : t + '@newsletter'; });

    if (validTargets.length === 0) {
        throw new Error('createNewsletterBridge: tidak ada target newsletter yang valid');
    }

    const filter    = typeof options.filter    === 'function' ? options.filter    : null;
    const transform = typeof options.transform === 'function' ? options.transform : null;
    const delayMs   = typeof options.delay     === 'number'   ? Math.max(0, options.delay) : 0;
    const skipOwn   = options.skipOwn !== false;
    const allowedTypes = Array.isArray(options.types) ? options.types : ['notify'];

    const doSleep = function(ms) { return new Promise(function(r) { return setTimeout(r, ms); }); };

    const defaultTransform = async function(msg, targetJid) {
        if (typeof sock.sendMessage !== 'function') return;
        // Coba forward dulu; kalau gagal pakai teks fallback
        try {
            await sock.sendMessage(targetJid, { forward: msg });
        } catch(e) {
            var teks = (msg.message && (
                msg.message.conversation ||
                (msg.message.extendedTextMessage && msg.message.extendedTextMessage.text) ||
                (msg.message.imageMessage && msg.message.imageMessage.caption) ||
                (msg.message.videoMessage && msg.message.videoMessage.caption)
            )) || '';
            if (teks) {
                await sock.sendMessage(targetJid, { text: teks });
            }
        }
    };

    const handler = async function(upsert) {
        var messages = (upsert && upsert.messages) || [];
        var type     = (upsert && upsert.type)     || '';
        if (allowedTypes.indexOf(type) === -1) return;

        for (var i = 0; i < messages.length; i++) {
            var msg = messages[i];
            if (!msg || !msg.key) continue;
            if (skipOwn && msg.key.fromMe) continue;
            // Jangan forward pesan yang berasal dari newsletter itu sendiri
            if (msg.key.remoteJid && msg.key.remoteJid.endsWith('@newsletter')) continue;

            if (filter) {
                var pass = false;
                try { pass = !!filter(msg, sock); } catch(e) { pass = false; }
                if (!pass) continue;
            }

            for (var j = 0; j < validTargets.length; j++) {
                var targetJid = validTargets[j];
                try {
                    if (transform) {
                        await transform(msg, targetJid, sock);
                    } else {
                        await defaultTransform(msg, targetJid);
                    }
                } catch(e) {
                    // jangan crash bot, lanjut ke pesan berikutnya
                }
                if (delayMs > 0 && j < validTargets.length - 1) {
                    await doSleep(delayMs);
                }
            }

            if (delayMs > 0 && i < messages.length - 1) {
                await doSleep(delayMs);
            }
        }
    };

    sock.ev.on('messages.upsert', handler);

    // Kembalikan fungsi cleanup supaya bot bisa matikan bridge kalau perlu
    return function removeBridge() {
        if (typeof sock.ev.off === 'function') {
            sock.ev.off('messages.upsert', handler);
        }
    };
};
exports.createNewsletterBridge = createNewsletterBridge;
