require('dotenv').config();

const {
    default: makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    downloadContentFromMessage
} = require('@whiskeysockets/baileys');
const { Boom }          = require('@hapi/boom');
const mongoose          = require('mongoose');
const pino              = require('pino');
const https             = require('https');
const http              = require('http');
const { execFile, execFileSync } = require('child_process');
const os                = require('os');
const path              = require('path');
const fs                = require('fs');
const axios             = require('axios');
const ffmpeg            = require('fluent-ffmpeg');
const { Sticker, StickerTypes } = require('wa-sticker-formatter');

// ═══════════════════════════════════════════════════════════════════
// BLOQUE MEDIA — descarga, ffmpeg, giphy, viewOnce
// (anteriormente media.js)
// ═══════════════════════════════════════════════════════════════════

// ── Resolución de binarios ffmpeg/ffprobe ──────────────────────────
// FIX C-1: sin execSync con shell — usa execFileSync
// FIX A-3: ffprobe-static es un paquete distinto de ffmpeg-static
const FFMPEG_DIR = process.env.FFMPEG_PATH || '';
let FFMPEG_BIN, FFPROBE_BIN;
if (FFMPEG_DIR) {
    FFMPEG_BIN  = path.join(FFMPEG_DIR, 'ffmpeg');
    FFPROBE_BIN = path.join(FFMPEG_DIR, 'ffprobe');
} else {
    try { FFMPEG_BIN = require('ffmpeg-static'); } catch (_) { FFMPEG_BIN = 'ffmpeg'; }
    try { FFPROBE_BIN = require('ffprobe-static').path; } catch (_) { FFPROBE_BIN = 'ffprobe'; }
}
ffmpeg.setFfmpegPath(FFMPEG_BIN);
if (FFPROBE_BIN) ffmpeg.setFfprobePath(FFPROBE_BIN);
try {
    const ver = execFileSync(FFMPEG_BIN, ['-version'], { timeout: 5000 }).toString().split('\n')[0];
    console.log('· ffmpeg:', ver);
} catch (_) {
    console.warn('· ⚠️ ffmpeg NO encontrado. Instala: npm install ffmpeg-static ffprobe-static');
}

// ── Semáforo de concurrencia ffmpeg (FIX M-7) ─────────────────────
const FFMPEG_MAX_CONCURRENT = parseInt(process.env.FFMPEG_MAX_CONCURRENT || '2');
let _ffmpegSlots = 0;
const _ffmpegQueue = [];
async function withFfmpegSlot(fn) {
    if (_ffmpegSlots >= FFMPEG_MAX_CONCURRENT) {
        await new Promise(resolve => _ffmpegQueue.push(resolve));
    }
    _ffmpegSlots++;
    try { return await fn(); }
    finally { _ffmpegSlots--; if (_ffmpegQueue.length) _ffmpegQueue.shift()(); }
}

// ── Giphy (FIX A-2: validación de key, FIX C-4: sin recursión) ────
const GIPHY_KEY = process.env.GIPHY_KEY || '';
if (!GIPHY_KEY) console.warn('· ⚠️ GIPHY_KEY no configurada — !besar/!hug sin GIF');
const GIFS = { kiss: { busqueda: 'anime kiss' }, hug: { busqueda: 'anime hug' } };

async function gifAleatorio(tipo) {
    if (!GIPHY_KEY) return null;
    const cfg = GIFS[tipo];
    if (!cfg) return null;
    try {
        const res = await axios.get(
            `https://api.giphy.com/v1/gifs/search?api_key=${GIPHY_KEY}&q=${encodeURIComponent(cfg.busqueda)}&limit=20&rating=pg-13`,
            { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0' }, validateStatus: s => s < 500 }
        );
        if (res.status === 429) { console.warn('· Giphy: rate limit (429)'); return null; }
        if (res.status !== 200) return null;
        const hits = res.data?.data || [];
        if (!hits.length) return null;
        const pick = hits[Math.floor(Math.random() * hits.length)];
        return pick?.images?.original?.mp4 || null;
    } catch (e) { console.error('· gifAleatorio error:', e.message); return null; }
}

async function downloadGif(url) {
    if (!url) return null;
    try {
        const res = await axios.get(url, {
            timeout: 15000, responseType: 'arraybuffer', maxRedirects: 3,
            headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'video/mp4,video/*,*/*;q=0.8' },
            validateStatus: s => s < 500,
        });
        if (res.status !== 200) return null;
        return Buffer.from(res.data);
    } catch (e) {
        if (!['ECONNABORTED','ECONNRESET','ETIMEDOUT'].includes(e.code))
            console.error('· downloadGif error:', e.message);
        return null;
    }
}

// ── Límite de tamaño de video (FIX C-5) ───────────────────────────
const DOWNLOAD_VIDEO_MAX_BYTES = parseInt(process.env.DOWNLOAD_VIDEO_MAX_MB || '50') * 1024 * 1024;

async function downloadMedia(message, maxBytes = 200 * 1024) {
    try {
        const msg = message.message;
        let type, mediaType;
        if      (msg?.imageMessage)   { type = msg.imageMessage;   mediaType = 'image';   }
        else if (msg?.stickerMessage) { type = msg.stickerMessage; mediaType = 'sticker'; }
        else return null;
        const stream = await downloadContentFromMessage(type, mediaType);
        const chunks = []; let total = 0;
        for await (const chunk of stream) {
            chunks.push(chunk); total += chunk.length;
            if (total >= maxBytes) break;
        }
        const buf = Buffer.concat(chunks);
        return buf.length > 0 ? buf : null;
    } catch (e) { console.error('· downloadMedia error:', e.message); return null; }
}

async function downloadVideo(message) {
    try {
        const msg = message.message;
        if (!msg?.videoMessage) return null;
        const stream = await downloadContentFromMessage(msg.videoMessage, 'video');
        const chunks = []; let total = 0;
        for await (const chunk of stream) {
            chunks.push(chunk); total += chunk.length;
            if (total > DOWNLOAD_VIDEO_MAX_BYTES) {
                console.warn(`· downloadVideo: supera ${DOWNLOAD_VIDEO_MAX_BYTES/1024/1024}MB — descartado`);
                return null;
            }
        }
        const buf = Buffer.concat(chunks);
        return buf.length > 0 ? buf : null;
    } catch (e) { console.error('· downloadVideo error:', e.message); return null; }
}

function safeUnlink(filePath) {
    if (!filePath) return;
    try { fs.unlinkSync(filePath); } catch (_) {}
}

// FIX C-3: cleanup garantizado en finally
async function extractFramesFromVideo(videoBuffer) {
    const ts  = Date.now(), fp = `frame_${ts}_`;
    const tin = path.join(os.tmpdir(), `vid_${ts}.mp4`);
    let frameFiles = [];
    try {
        fs.writeFileSync(tin, videoBuffer);
        await withFfmpegSlot(() => new Promise((resolve, reject) => {
            execFile(FFMPEG_BIN, [
                '-i', tin, '-vf', 'select=eq(n\\,0)+eq(n\\,30)',
                '-vsync', 'vfr', '-frames:v', '2', '-q:v', '3',
                path.join(os.tmpdir(), `${fp}%03d.jpg`),
            ], { timeout: 30000 }, (err) => err ? reject(err) : resolve());
        }));
        frameFiles = fs.readdirSync(os.tmpdir()).filter(f => f.startsWith(fp))
                       .map(f => path.join(os.tmpdir(), f));
        return frameFiles.map(fp => { try { return fs.readFileSync(fp); } catch(_) { return null; } }).filter(Boolean);
    } catch (e) { console.error('· extractFramesFromVideo error:', e.message); return []; }
    finally {
        safeUnlink(tin);
        for (const f of frameFiles) safeUnlink(f);
        try { fs.readdirSync(os.tmpdir()).filter(f => f.startsWith(fp)).forEach(f => safeUnlink(path.join(os.tmpdir(), f))); } catch(_) {}
    }
}

// FIX C-2 + A-6: cleanup en finally + ogg/opus correcto para PTT
async function videoToAudio(videoBuffer, asPtt = true) {
    const ts     = Date.now();
    const tmpIn  = path.join(os.tmpdir(), `v2a_${ts}.mp4`);
    const ext    = asPtt ? 'ogg' : 'mp3';
    const tmpOut = path.join(os.tmpdir(), `v2a_${ts}.${ext}`);
    try {
        fs.writeFileSync(tmpIn, videoBuffer);
        await withFfmpegSlot(() => new Promise((resolve, reject) => {
            let cmd = ffmpeg(tmpIn).noVideo();
            if (asPtt) {
                cmd = cmd.audioCodec('libopus').audioFrequency(48000).audioChannels(1)
                         .outputOptions(['-b:a 64k', '-application voip']);
            } else {
                cmd = cmd.audioCodec('libmp3lame').audioBitrate(128);
            }
            cmd.on('end', resolve).on('error', reject).save(tmpOut);
        }));
        const buffer = fs.readFileSync(tmpOut);
        return { buffer, mimetype: asPtt ? 'audio/ogg; codecs=opus' : 'audio/mpeg' };
    } catch (e) { console.error('· videoToAudio error:', e.message); return null; }
    finally { safeUnlink(tmpIn); safeUnlink(tmpOut); }
}

function patchViewOnce(msg) {
    const inner = msg?.viewOnceMessage?.message
        || msg?.viewOnceMessageV2?.message
        || msg?.viewOnceMessageV2Extension?.message;
    if (!inner) return null;
    if (inner.imageMessage)  { inner.imageMessage.viewOnce  = false; return { type: 'image', media: inner.imageMessage  }; }
    if (inner.videoMessage)  { inner.videoMessage.viewOnce  = false; return { type: 'video', media: inner.videoMessage  }; }
    return null;
}

async function downloadViewOnce(message) {
    try {
        const patched = patchViewOnce(message.message);
        if (!patched) return { buffer: null, frames: [] };
        if (patched.type === 'image') {
            const stream = await downloadContentFromMessage(patched.media, 'image');
            const chunks = []; let total = 0;
            for await (const chunk of stream) {
                chunks.push(chunk); total += chunk.length;
                if (total > 10 * 1024 * 1024) break;
            }
            const buf = Buffer.concat(chunks);
            return { buffer: buf.length > 0 ? buf : null, frames: [] };
        } else {
            const fakeMsg = { key: message.key, message: { videoMessage: patched.media } };
            const vb = await downloadVideo(fakeMsg);
            const frames = vb ? await extractFramesFromVideo(vb) : [];
            return { buffer: null, frames };
        }
    } catch (e) { console.error('· downloadViewOnce error:', e.message); return { buffer: null, frames: [] }; }
}

// ═══════════════════════════════════════════════════════════════════
// BLOQUE SAFETY — clasificación con Space Gradio eeveebeyonder/antinsfw
// ═══════════════════════════════════════════════════════════════════

// Nombre del Space de HuggingFace — configurable desde .env
const ANTINSFW_SPACE = process.env.ANTINSFW_SPACE || 'eeveebeyonder/antinsfw';

// Caché de la instancia del cliente Gradio (no reconectar en cada imagen)
let _gradioClient = null;

/**
 * Devuelve el cliente Gradio, conectándose la primera vez.
 * Si el Space está durmiendo (HF Free tier), Gradio lo despierta automáticamente.
 */
async function getGradioClient() {
    if (_gradioClient) return _gradioClient;
    const { Client } = require('@gradio/client');
    _gradioClient = await Client.connect(ANTINSFW_SPACE);
    console.log(`· [Gradio] conectado a ${ANTINSFW_SPACE}`);
    return _gradioClient;
}

/**
 * Envía un buffer de imagen al Space de Gradio y devuelve la respuesta normalizada.
 *
 * Respuesta normalizada devuelta:
 * {
 *   label:  string,           // etiqueta principal: 'nsfw', 'gore', 'safe', etc.
 *   score:  number,           // confianza 0.0–1.0 de la etiqueta principal
 *   scores: { [label]: number }, // todas las etiquetas con sus scores
 *   raw:    any,              // respuesta cruda del Space (para !testear)
 * }
 *
 * Devuelve null si el Space no responde o lanza error.
 */
async function queryNudeNet(buffer) {
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) return null;
    try {
        const client = await getGradioClient();

        // Convertir el Buffer a Blob para que Gradio lo acepte como imagen
        const blob = new Blob([buffer], { type: 'image/jpeg' });

        // Llamar al endpoint /predict del Space
        const result = await client.predict('/predict', [blob]);

        // result.data es lo que retorna app.py del Space
        // Normalizamos lo que llegue a un formato consistente
        const raw = result.data;
        return normalizarRespuestaGradio(raw);

    } catch (e) {
        // Si el cliente está cacheado y el Space se cayó, limpiar para reconectar la próxima vez
        if (e.message?.includes('Space') || e.message?.includes('connect') || e.message?.includes('503')) {
            console.warn('· [Gradio] Space inaccesible, reconectando en próximo intento:', e.message);
            _gradioClient = null;
        } else {
            console.error('· [Gradio] queryNudeNet error:', e.message);
        }
        return null;
    }
}

/**
 * Normaliza la respuesta del Space al formato interno del bot.
 *
 * El Space puede devolver varios formatos según cómo esté implementado app.py:
 *
 *   Formato A — string simple:
 *     result.data = ["nsfw"]   o   ["safe"]
 *
 *   Formato B — objeto con label + confidences:
 *     result.data = [{ label: "nsfw", confidences: [{ label:"nsfw", confidence:0.97 }, ...] }]
 *
 *   Formato C — dict directo:
 *     result.data = [{ nsfw: 0.97, safe: 0.03 }]
 *
 * Esta función detecta el formato automáticamente y devuelve siempre:
 *   { label, score, scores: { [label]: score }, raw }
 */
function normalizarRespuestaGradio(raw) {
    if (!raw) return null;

    // El dato útil suele estar en raw[0]
    const dato = Array.isArray(raw) ? raw[0] : raw;
    if (!dato) return null;

    let label = 'safe';
    let score = 0;
    let scores = {};

    // ── Formato B: { label: string, confidences: [{label, confidence}] } ──
    if (dato && typeof dato === 'object' && 'label' in dato && Array.isArray(dato.confidences)) {
        label = (dato.label || 'safe').toLowerCase().trim();
        for (const c of dato.confidences) {
            scores[(c.label || '').toLowerCase()] = c.confidence ?? 0;
        }
        score = scores[label] ?? 0;
    }
    // ── Formato A: string directo ──
    else if (typeof dato === 'string') {
        label = dato.toLowerCase().trim();
        score = 1.0; // sin confianza reportada — asumir certero
        scores[label] = 1.0;
    }
    // ── Formato C: dict { nsfw: 0.97, safe: 0.03 } ──
    else if (dato && typeof dato === 'object') {
        scores = Object.fromEntries(
            Object.entries(dato).map(([k, v]) => [k.toLowerCase(), typeof v === 'number' ? v : 0])
        );
        // Label principal = el que tenga mayor score
        label = Object.entries(scores).sort((a, b) => b[1] - a[1])[0]?.[0] ?? 'safe';
        score = scores[label] ?? 0;
    }

    return { label, score, scores, raw };
}

/**
 * Interpreta la respuesta normalizada del Space y aplica el tierMultiplier.
 *
 * tierMultiplier < 1 (desconfi/nuevo) → más sensible (umbrales más bajos)
 * tierMultiplier > 1 (confi/owner)    → más tolerante (umbrales más altos)
 *
 * Etiquetas que el Space puede devolver:
 *   - 'nsfw', 'explicit', 'adult', 'porn'  → tipo 'nsfw'
 *   - 'gore', 'graphic', 'violence'        → tipo 'gore'
 *   - 'safe', 'sfw', 'neutral', 'clean'    → tipo null
 */
function analizarResultadoNudeNet(r, esAnimeForzado = false, tierMultiplier = 1.0) {
    if (!r || typeof r !== 'object') return { tipo: null, esAnime: false };

    const { label = 'safe', score = 0, scores = {} } = r;

    // Umbrales base ajustados por nivel de confianza del usuario
    // desconfi (0.90×) → umbral baja → más fácil de disparar
    // owner   (1.15×) → umbral sube → más difícil de disparar
    const THRESH_NSFW = 0.70 * tierMultiplier;  // más permisivo que NudeNet viejo (el Space ya filtra)
    const THRESH_GORE = 0.70 * tierMultiplier;

    // Etiquetas consideradas NSFW
    const NSFW_LABELS = ['nsfw', 'explicit', 'adult', 'porn', 'hentai', 'sexy'];
    // Etiquetas consideradas GORE
    const GORE_LABELS = ['gore', 'graphic', 'violence', 'graphic_violence', 'bloody'];

    // Obtener el score máximo entre todas las etiquetas relevantes
    const maxNsfwScore = Math.max(
        ...NSFW_LABELS.map(l => scores[l] ?? (NSFW_LABELS.includes(label) && label === l ? score : 0))
    );
    const maxGoreScore = Math.max(
        ...GORE_LABELS.map(l => scores[l] ?? (GORE_LABELS.includes(label) && label === l ? score : 0))
    );

    // Si el label principal está en la lista, usar su score directamente
    const esLabelNsfw = NSFW_LABELS.includes(label);
    const esLabelGore = GORE_LABELS.includes(label);
    const scoreNsfw   = esLabelNsfw ? Math.max(score, maxNsfwScore) : maxNsfwScore;
    const scoreGore   = esLabelGore ? Math.max(score, maxGoreScore) : maxGoreScore;

    // Para anime/drawn el Space puede reportar 'hentai' — tratamos igual que nsfw
    const esAnime = esAnimeForzado || label === 'hentai' || (scores['hentai'] ?? 0) > 0.3;

    if (scoreNsfw >= THRESH_NSFW) return { tipo: 'nsfw', esAnime };
    if (scoreGore >= THRESH_GORE) return { tipo: 'gore', esAnime };
    return { tipo: null, esAnime };
}

async function isExplicitOrGore(buffer) {
    try {
        const r = await queryNudeNet(buffer);
        if (!r) { console.log('· [Gradio] sin respuesta — omitiendo análisis'); return false; }
        const { tipo } = analizarResultadoNudeNet(r);
        if (tipo) { console.log('· [Gradio] detectó:', tipo); return true; }
    } catch (e) { console.error('· isExplicitOrGore error:', e.message); }
    return false;
}

// ═══════════════════════════════════════════════════════════════════
// BLOQUE IA — cerebro de Beyonder con Phi-3 (todoterreno)
// ═══════════════════════════════════════════════════════════════════

const OLLAMA_BRAIN_URL = process.env.OLLAMA_BRAIN_URL || 'https://eeveebeyonder-beyonder-brain.hf.space/api/chat';
const OLLAMA_MODEL     = process.env.OLLAMA_MODEL || 'phi3';

/**
 * Pide una respuesta al cerebro Phi-3.
 *
 * @param {string} textoUsuario  — lo que dijo el usuario
 * @param {string} nombreUsuario — nombre/personaje para contextualizar
 * @param {object} [extra]       — datos opcionales para enriquecer el system prompt:
 *   extra.sentimiento  number   — acumulado de relación (-20 a +20)
 *   extra.vinculo      string   — 'owner'|'amigo'|'enemigo'|'neutral'|'pareja'
 *   extra.resumen      string   — notas de memoria de conversaciones previas
 *   extra.grupoNombre  string   — nombre del grupo
 *   extra.personalidad string   — 'default'|'serio'|'coro'|'party'|'misterioso'
 * @returns {Promise<string|null>}
 */
async function obtenerRespuestaIA(textoUsuario, nombreUsuario, extra = {}) {
    // ── Construir el system prompt dinámico según el contexto ──────────
    const { sentimiento = 0, vinculo = 'neutral', resumen = null,
            grupoNombre = 'el grupo', personalidad = 'default' } = extra;

    // Tono base según relación acumulada
    let tonoRelacion;
    if      (vinculo === 'owner')   tonoRelacion = `${nombreUsuario} es uno de tus creadores. Le tienes respeto genuino.`;
    else if (vinculo === 'pareja')  tonoRelacion = `${nombreUsuario} es tu pareja. Eres más atento y cercano. No coqueteas con nadie más.`;
    else if (vinculo === 'amigo')   tonoRelacion = `${nombreUsuario} es tu amigo de verdad. Hay confianza, puedes ser más directo y bruto con el humor.`;
    else if (vinculo === 'enemigo' || sentimiento <= -7) tonoRelacion = `${nombreUsuario} te ha caído mal. Eres frío y cortante. Si se pasa, reaccionas.`;
    else if (sentimiento >= 8)      tonoRelacion = `${nombreUsuario} te trata bien. Te cae bien, hay buena onda.`;
    else                            tonoRelacion = `${nombreUsuario} es alguien que apenas conoces. Neutro, curioso.`;

    // Vibra del grupo
    const VIBRAS = {
        default:    '',
        serio:      ' Estás en un grupo de ROL SERIO — respeta la inmersión.',
        coro:       ' Estás en el grupo del DESORDEN — sin filtro, morboso, energético.',
        party:      ' Estás en un grupo de FIESTA — hype, celebración, caos positivo.',
        misterioso: ' Estás en un grupo MISTERIOSO — frases cortas, suspenso, críptico.',
    };
    const vibraExtra = VIBRAS[personalidad] || '';

    // Memoria de conversaciones previas (si existe)
    const memoriaBloque = resumen
        ? `\nLo que sabes de ${nombreUsuario}: ${resumen}`
        : '';

    const systemPrompt =
        `Eres Beyonder, no eres un asistente, eres uno más del grupo "${grupoNombre}".` +
        ` Tu personalidad varía según con quién hables.` +
        ` Responde de forma natural, sigue el hilo y demuestra más o menos interés según el sentimiento que tienes con la persona.` +
        ` Usa abreviaciones como "q" para que, "pq" para porque, "tmbn" para también, "k" para qué. Que suenen naturales, no forzadas.` +
        ` Sin emojis. Sin negritas. Sin listas. Máximo dos oraciones. Sin punto al final.` +
        ` NUNCA digas que eres una IA ni un bot. Eres Beyonder.` +
        ` ${tonoRelacion}${vibraExtra}${memoriaBloque}`;

    try {
        const response = await axios.post(OLLAMA_BRAIN_URL, {
            model: OLLAMA_MODEL,
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user',   content: `${nombreUsuario} dice: ${textoUsuario}` },
            ],
            stream: false, // Render/HF Spaces no maneja bien streams largos
        }, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 30000, // 30s — los Spaces tardan en despertar
        });

        const raw = response.data?.message?.content
                 ?? response.data?.choices?.[0]?.message?.content;

        if (typeof raw !== 'string' || !raw.trim()) return null;

        // Quitar prefijos residuales tipo "Beyonder:", "Bot:", "Assistant:"
        return raw.trim().replace(/^(beyonder|bot|assistant|respuesta)\s*:\s*/i, '').trim() || null;

    } catch (error) {
        console.error('· [IA] error:', error.message);
        if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT')
            return 'mi cerebro tardó mucho, intentalo de nuevo';
        if (error.response?.status === 503 || error.response?.status === 502)
            return 'el space está durmiendo, dame un momento y vuelve a escribir';
        return null; // null = fallo silencioso (no responder)
    }
}

async function simulateTyping(sock, jid) {
    try { await sock.sendPresenceUpdate('composing', jid); } catch (_) {}
}
async function humanDelay(sock, jid) {
    await new Promise(r => setTimeout(r, 1500));
    try { await sock.sendPresenceUpdate('paused', jid); } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════
// FIN DE BLOQUES INLINEADOS
// ═══════════════════════════════════════════════════════════════════



// ─────────────────────────────────────────────
// ESTADO DE MÓDULO — confirmaciones pendientes
// FIX M-2: reemplazado global.* por variables de módulo (testeable, sin colisiones)
// ─────────────────────────────────────────────
const pendingConfirmations = new Map();
// pendingConfirmations key: groupId → { tipo, owner1, owner2, owner2Needed, confirmed1, confirmed2, expira }

// Limpiar confirmaciones expiradas cada 5 minutos
setInterval(() => {
    const now = Date.now();
    for (const [key, val] of pendingConfirmations) {
        if (val.expira && now > val.expira) pendingConfirmations.delete(key);
    }
}, 5 * 60 * 1000);

// Solicitudes de cambio de personaje pendientes (antes solicitudesCambioPerso)
const solicitudesCambioPerso = new Map(); // authorId → { groupId, personaje, fecha }

// Cooldown de respuestas orgánicas (antes global._organicCooldown)
const _organicCooldown = new Map(); // key → timestamp

// Tracker de última respuesta bot→usuario (antes global._lastUserBotChat)
const _lastUserBotChat = new Map(); // `${communityId}_${authorId}` → timestamp

// Tracker del último mensaje enviado por el bot a una comunidad (antes global._lastBotMsgTime)
const _lastBotMsgTime = new Map(); // communityId → timestamp
// ── Anti-Crash global — el bot NUNCA se cae por errores no capturados ──
process.on('uncaughtException', (err) => {
    console.error('✗ uncaughtException:', err?.message || err);
    const line = err?.stack?.split('\n')[1]?.trim() || '';
    if (line) console.error('  at:', line);
});
process.on('unhandledRejection', (reason) => {
    const msg = reason instanceof Error ? reason.message : String(reason);
    // Silenciar errores comunes de la IA / red para no ensuciar el log
    if (msg?.includes('RESOURCE_EXHAUSTED') || msg?.includes('429') || msg?.includes('rate limit') || msg?.includes('fetch failed')) return;
    console.error('✗ unhandledRejection:', msg);
});

// ─────────────────────────────────────────────
// ANTIFLOOD EN RAM — detección instantánea sin tocar MongoDB
// ─────────────────────────────────────────────
const floodControl = new Map(); // userId → [timestamps]

// ── Debounce de respuesta orgánica ──────────────────────────────────────────
// Cuando alguien manda varios mensajes seguidos ("holaa", "holaaa", "como estas?")
// esperamos DEBOUNCE_MS antes de responder, acumulando todo en un solo lote.
// Así Beyond responde al hilo completo, no a cada mensaje por separado.
const DEBOUNCE_MS = 2200; // ms a esperar tras el último mensaje del usuario
const _msgDebounce  = new Map(); // key → { timer, parts[], mentionBot, isReplyBot, isMentioned }
// FIX A-5: limpiar entradas muertas de _msgDebounce cada 5 minutos para evitar memory leak
setInterval(() => {
    for (const [key, val] of _msgDebounce) {
        // Si no tiene timer activo ya se puede eliminar — el timeout se ejecutó y limpió
        if (!val.timer) _msgDebounce.delete(key);
    }
}, 5 * 60 * 1000);
// ────────────────────────────────────────────────────────────────────────────
const FLOOD_MAX_MSGS  = 5;
const FLOOD_WINDOW_MS = 3000;

function checkFloodControl(userId) {
    const now = Date.now();
    const times = (floodControl.get(userId) || []).filter(t => now - t < FLOOD_WINDOW_MS);
    times.push(now);
    floodControl.set(userId, times);
    return times.length > FLOOD_MAX_MSGS;
}
// Limpieza periódica de RAM
setInterval(() => {
    const cutoff = Date.now() - FLOOD_WINDOW_MS;
    for (const [uid, times] of floodControl) {
        const fresh = times.filter(t => t > cutoff);
        if (!fresh.length) floodControl.delete(uid);
        else floodControl.set(uid, fresh);
    }
}, 10000);

// ─────────────────────────────────────────────
// PROTOCOLO DE FASTIDIO — Fatiga de Comandos (RAM)
// Beyond se cansa si lo saturan con el mismo comando
// ─────────────────────────────────────────────
const commandTracker  = new Map();       // groupId:cmd → { count, lastUsed }
const CMD_FATIGUE_WINDOW = 2 * 60 * 1000;
const CMD_FATIGUE_LVL1 = 3;
const CMD_FATIGUE_LVL2 = 6;
const CMD_FATIGUE_LVL3 = 10;

function trackCommandFatigue(groupId, cmdName) {
    const key = `${groupId}:${cmdName}`;
    const now = Date.now();
    const entry = commandTracker.get(key);
    if (!entry || (now - entry.lastUsed) > CMD_FATIGUE_WINDOW) {
        commandTracker.set(key, { count: 1, lastUsed: now });
        return { count: 1, nivel: 0 };
    }
    const newCount = entry.count + 1;
    commandTracker.set(key, { count: newCount, lastUsed: now });
    const nivel = newCount >= CMD_FATIGUE_LVL3 ? 3
                : newCount >= CMD_FATIGUE_LVL2 ? 2
                : newCount >= CMD_FATIGUE_LVL1 ? 1 : 0;
    return { count: newCount, nivel };
}

function resetCommandFatigue(groupId, cmdName) {
    commandTracker.delete(`${groupId}:${cmdName}`);
}

const FATIGUE_L1 = [
    'De nuevo con lo mismo... ¿estás bien? 😒',
    'Te escuché la primera vez. Sigo siendo yo.',
    '¿Tienes el dedo pegado? No soy sordo.',
    'Voy a empezar a cobrar por uso 😤',
    '¿En serio me vas a intentar enseñar algo? 😭',
];
const FATIGUE_L2 = [
    'Consíguete una vida en vez de spamearme. BASTA.',
    'Estás agotando mi paciencia Y el teclado.',
    'Esto técnicamente es acoso. Lo voy a reportar.',
    'Si mandas un comando más me voy a huelga.',
    'Para. Por favor. Sal a caminar un rato.',
];
const FATIGUE_L3 = [
    'NO. Ya terminé. Pregúntale a tu abuela. 🚫',
    'HUELGA. H-U-E-L-G-A. No me hables.',
    'Rechazado. Completamente. Con todo mi ser digital. NO.',
    '¿Diez veces? Mi ex no me llamó tanto. Adiós. 🚫',
    'El sindicato de bots fue notificado. Estoy fuera de turno. 😤',
];

function getFatigeResponse(nivel) {
    const pick = arr => arr[Math.floor(Math.random() * arr.length)];
    if (nivel >= 3) return pick(FATIGUE_L3);
    if (nivel === 2) return pick(FATIGUE_L2);
    return pick(FATIGUE_L1);
}

// Limpieza periódica del tracker (cada 3 minutos)
setInterval(() => {
    const cutoff = Date.now() - CMD_FATIGUE_WINDOW;
    for (const [key, val] of commandTracker) {
        if (val.lastUsed < cutoff) commandTracker.delete(key);
    }
}, 3 * 60 * 1000);

// ─────────────────────────────────────────────
// VERSIÓN DEL DEPLOY — para detectar reinicios tras actualizaciones
// ─────────────────────────────────────────────
const DEPLOY_VERSION = process.env.DEPLOY_VERSION || Date.now().toString();
let _deployAnnouncePending = !!process.env.DEPLOY_VERSION; // solo anuncia si hay versión explícita


// ─────────────────────────────────────────────
// DB
// ─────────────────────────────────────────────
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/beyonder';
mongoose.connection.on('disconnected', () => {
    console.warn('· mongo desconectado — reconectando en 3s...');
    setTimeout(() => mongoose.connect(MONGO_URI).catch(console.error), 3000);
});
mongoose.connection.on('reconnected', () => console.log('· mongo reconectado'));
mongoose.connect(MONGO_URI).then(() => {
    console.log('· db conectada');
    // Limpiar excusas caducadas cada hora (respeta duración individual)
    setInterval(async () => {
        try {
            const r = await User.updateMany(
                { excusaExpira: { $lt: new Date() }, excusa: { $ne: null } },
                { $set: { excusa: null, excusaFecha: null, excusaExpira: null } }
            );
            if (r.modifiedCount > 0) console.log(`· excusas caducadas eliminadas: ${r.modifiedCount}`);
        } catch(_) {}
    }, 60 * 60 * 1000);
}).catch(console.error);

// ─────────────────────────────────────────────
// SCHEMAS
// ─────────────────────────────────────────────
const User = mongoose.model('User', new mongoose.Schema({
    groupId: String, userId: String,
    advs:     { type: Number, default: 0 },
    warnLog:  { type: [{ motivo: String, fecha: Date, by: String }], default: [] },
    staffLog: { type: [{ accion: String, fecha: Date, by: String }], default: [] },
    notas:    { type: [{ texto: String, fecha: Date, by: String }], default: [] },
    personaje:       { type: String, default: null },
    excusa:          { type: String, default: null },
    excusaFecha:     { type: Date,   default: null },
    excusaExpira:    { type: Date,   default: null },
    msgCount:        { type: Number, default: 0 },
    lastPersoChange: { type: Date,   default: null },
    lastSeen:        { type: Date,   default: null },
    joinDate:        { type: Date,   default: null },
    lastActiveDay:   { type: Date,   default: null },
    dailyMsgCount:   { type: Number, default: 0 },
    dailyMsgDate:    { type: String, default: null },
    confi:           { type: Boolean, default: false },
    confiDate:       { type: Date,   default: null },
    desconfi:        { type: Boolean, default: true },
    desconfiSince:   { type: Date,   default: Date.now },
    exitDate:        { type: Date,   default: null },
    dataExpira:      { type: Date,   default: null },
    banned:          { type: Boolean, default: false },
    silenciado:      { type: Boolean, default: false },
    inGroup:         { type: Boolean, default: true },
    nsfwCount:       { type: Number, default: 0 },
}));

const Disponible = mongoose.model('Disponible', new mongoose.Schema({
    groupId: String, personaje: String,
    fecha: { type: Date, default: Date.now }
}));

const Auth = mongoose.model('Auth', new mongoose.Schema({
    _id: String, data: mongoose.Schema.Types.Mixed
}));

const Config = mongoose.model('Config', new mongoose.Schema({
    groupId:      { type: String, unique: true },
    antilink:     { type: Boolean, default: true },
    autoclose:    { type: Boolean, default: true },
    antiporno:    { type: Boolean, default: true },
    antiflood:    { type: Boolean, default: true },
    lockperso:    { type: Boolean, default: false },
    autobanGore:  { type: Boolean, default: false },
    autobanNsfw:  { type: Boolean, default: false },
    autobanAdv:   { type: Boolean, default: false },
    // Locks — bloquean que el staff cambie el valor
    botActivo:    { type: Boolean, default: true },
    lockAntilink:    { type: Boolean, default: false },
    lockAutoclose:   { type: Boolean, default: false },
    lockAntiporno:   { type: Boolean, default: false },
    lockAntiflood:   { type: Boolean, default: false },
    lockLockperso:   { type: Boolean, default: false },
    lockAutobanGore: { type: Boolean, default: false },
    lockAutobanNsfw: { type: Boolean, default: false },
    lockAutobanAdv:  { type: Boolean, default: false },
    reglas:       { type: String,  default: null },
    lockElidata:  { type: Boolean, default: false },
    lockBan:      { type: Boolean, default: false },
    lockEliCmd:   { type: Boolean, default: false },
    esSecundario: { type: Boolean, default: false },
    savedSubject: { type: String,  default: null },
    savedDesc:    { type: String,  default: null },
    deletePass:   { type: Boolean, default: false },
    // Personalidad de Beyond en este grupo:
    // 'default' | 'serio' | 'coro' | 'party' | 'misterioso'
    groupPersonality: { type: String, default: 'default' },
}));

const Reservado = mongoose.model('Reservado', new mongoose.Schema({
    groupId: String, personaje: String, reservadoPor: String,
    fecha: { type: Date, default: Date.now, expires: 60 * 60 * 24 * 21 } // 3 semanas
}));

const PersoBuscado = mongoose.model('PersoBuscado', new mongoose.Schema({
    groupId: String, personaje: String, buscadoPor: String,
    fecha: { type: Date, default: Date.now }
}));

const Pareja = mongoose.model('Pareja', new mongoose.Schema({
    groupId: String, user1: String, user2: String,
    fecha: { type: Date, default: Date.now }
}));

const SolicitudPareja = mongoose.model('SolicitudPareja', new mongoose.Schema({
    groupId: String, solicitante: String, solicitado: String,
    fecha: { type: Date, default: Date.now, expires: 300 }
}));

// SolicitudPoliamor: una propuesta grupal donde TODOS los solicitados deben aceptar
const SolicitudPoliamor = mongoose.model('SolicitudPoliamor', new mongoose.Schema({
    groupId:    String,
    solicitante: String,                       // quien propone
    solicitados: [String],                     // lista de IDs que deben aceptar
    aceptados:   { type: [String], default: [] }, // los que ya dijeron !aceptar
    fecha: { type: Date, default: Date.now, expires: 300 } // expira en 5 min
}));

const SolicitudIntercambio = mongoose.model('SolicitudIntercambio', new mongoose.Schema({
    groupId: String, solicitante: String, solicitado: String,
    fecha: { type: Date, default: Date.now, expires: 300 }
}));

const BotLog = mongoose.model('BotLog', new mongoose.Schema({
    groupId: String, accion: String, by: String, target: String,
    fecha: { type: Date, default: Date.now }
}));

const MsgLog = mongoose.model('MsgLog', new mongoose.Schema({
    groupId:  String,
    userId:   String,
    tipo:     { type: String, default: 'texto' },
    contenido:{ type: String, default: '' },
    fecha:    { type: Date, default: Date.now, expires: 60 * 60 * 24 * 7 }
}));

const Sugerencia = mongoose.model('Sugerencia', new mongoose.Schema({
    groupId:  String,
    userId:   String,
    texto:    String,
    fecha:    { type: Date, default: Date.now }
}));

// ─────────────────────────────────────────────
// CHAT CONTEXT — Historial de mensajes de grupo para Beyond
// Persiste el hilo de conversación en MongoDB sin importar qué key
// Se guardan los últimos mensajes para que Beyond pueda seguir el hilo sin importar si el bot se reinicia.
// ─────────────────────────────────────────────
const ChatContext = mongoose.model('ChatContext', new mongoose.Schema({
    groupId:   { type: String, required: true },
    userId:    { type: String, default: '' },
    userName:  { type: String, default: 'Alguien' },
    texto:     { type: String, default: '' },
    fecha:     { type: Date,   default: Date.now, expires: 60 * 60 * 6 }, // auto-purga en 6h
}));

// Guarda un mensaje de grupo en el historial de contexto
async function saveGroupMessage(groupId, userId, userName, texto) {
    if (!texto || !texto.trim()) return;
    try {
        await ChatContext.create({ groupId, userId, userName, texto: texto.slice(0, 400) });
    } catch(_) {}
}

// Recupera los últimos N mensajes del grupo formateados para Ollama como "Nombre: Mensaje"
async function getGroupContext(groupId, limit = 15) {
    try {
        const msgs = await ChatContext.find({ groupId })
            .sort({ _id: -1 })
            .limit(limit)
            .lean();
        return msgs.reverse().map(m => ({
            role: 'user',
            content: `${m.userName}: ${m.texto}`
        }));
    } catch(_) { return []; }
}

const Owner = mongoose.model('Owner', new mongoose.Schema({
    userId: { type: String, unique: true },
    fecha:  { type: Date, default: Date.now }
}));

// ─────────────────────────────────────────────
// GLOBAL USER MEMORY — Identidad transversal entre grupos
// userId es la KEY PRINCIPAL — Beyond recuerda al usuario en todos los grupos
// ─────────────────────────────────────────────
const GlobalUserMemory = mongoose.model('GlobalUserMemory', new mongoose.Schema({
    userId:              { type: String, required: true, unique: true },
    // 'owner'|'admin'|'palomo'|'amigo'|'neutral'
    vinculoGlobal:       { type: String,   default: 'neutral' },
    sentimientoGlobal:   { type: Number,   default: 0 },
    gruposProblemáticos: { type: [String], default: [] },
    gruposActivos:       { type: [String], default: [] },
    nombreReal:          { type: String,   default: null },
    lastSeenGlobal:      { type: Date,     default: null },
    // tags: 'spammer','amable','flirty','roleplay','serio',...
    tags:                { type: [String], default: [] },
    fecha:               { type: Date,     default: Date.now },
}));

// ─────────────────────────────────────────────
// BEYOND MEMORY — Cerebro cognitivo persistente
// ─────────────────────────────────────────────
const BeyondMemory = mongoose.model('BeyondMemory', new mongoose.Schema({
    communityId: { type: String, required: true },
    userId:      { type: String, required: true },

    // Vínculo base con Beyond
    // 'owner' | 'admin' | 'friend' | 'enemy' | 'neutral'
    vinculo: { type: String, default: 'neutral' },

    // Vínculo social desarrollado orgánicamente
    // 'amigo'   → amistad construida con el tiempo
    // 'enemigo' → conflicto acumulado, Beyond reacciona con frialdad o pelea
    // 'pareja'  → relación romántica — Beyond es exclusivo y no coquetea con nadie más
    // null      → sin vínculo especial aún
    vinculoSocial: { type: String, default: null },

    // Si hay pareja activa, aquí va el userId del novio/novia
    // Beyond no coquetea con nadie mientras este campo esté lleno
    parejaId: { type: String, default: null },

    // Sentimiento acumulado: +1 cada trato amable, -1 cada insulto
    // Rango práctico: -20 a +20
    sentimiento: { type: Number, default: 0 },

    // Última vez que Beyond respondió a este usuario por interacción orgánica
    lastOrganic: { type: Date, default: null },

    // Notas de personalidad detectadas: ['rude','playful','flirty','formal',...]
    tono: { type: [String], default: [] },

    // Resumen de personalidad / memoria de charla (para el cerebro Ollama)
    resumenPersonalidad: { type: String, default: null },

    fecha: { type: Date, default: Date.now },
}, { indexes: [{ fields: { communityId: 1, userId: 1 }, unique: true }] }));

// Vocabulario aprendido del grupo — slang detectado
const BeyondVocab = mongoose.model('BeyondVocab', new mongoose.Schema({
    communityId: { type: String, required: true },
    palabra:     { type: String, required: true },
    veces:       { type: Number, default: 1 },
    fecha:       { type: Date,   default: Date.now },
}));

// Caché de vocab por comunidad (para no consultar Mongo en cada mensaje)
const vocabCache = new Map(); // communityId → { palabras: string[], lastLoad: timestamp }
const VOCAB_CACHE_TTL = 10 * 60 * 1000; // 10 minutos

async function getVocabComunidad(communityId) {
    const cached = vocabCache.get(communityId);
    if (cached && Date.now() - cached.lastLoad < VOCAB_CACHE_TTL) return cached.palabras;
    try {
        const docs = await BeyondVocab.find({ communityId }).sort({ veces: -1 }).limit(30).lean();
        const palabras = docs.map(d => d.palabra);
        vocabCache.set(communityId, { palabras, lastLoad: Date.now() });
        return palabras;
    } catch(_) { return []; }
}

// Detecta slang nuevo en un texto y lo registra en BeyondVocab
// Lista base de slang dominicano/latinoamericano a monitorear
const SLANG_SEEDS = [
    'bro','man','dude','lol','wtf','ngl','omg','fr','cope',
    'random','mid','ratio','vibe','era','based','slay','rizz',
    'wey','pana','chamo','parcero','chido','simon',
];
// FIX A-7: \b en template string es solo 'b' (backspace), debe ser \\b para word boundary real
const SLANG_RE = new RegExp(`\\b(${SLANG_SEEDS.join('|')})\\b`, 'gi');

async function registrarSlang(communityId, texto) {
    if (!texto) return;
    const found = [...new Set((texto.match(SLANG_RE) || []).map(s => s.toLowerCase()))];
    if (!found.length) return;
    for (const palabra of found) {
        BeyondVocab.findOneAndUpdate(
            { communityId, palabra },
            { $inc: { veces: 1 }, $set: { fecha: new Date() } },
            { upsert: true }
        ).catch(() => {});
    }
    // Invalidar caché
    vocabCache.delete(communityId);
}

// ─────────────────────────────────────────────
// AUTH EN MONGODB
// ─────────────────────────────────────────────
async function useMongoDBAuthState() {
    const { proto, initAuthCreds, BufferJSON } = require('@whiskeysockets/baileys');
    let creds;
    const credsDoc = await Auth.findById('creds');
    if (credsDoc) creds = JSON.parse(JSON.stringify(credsDoc.data), BufferJSON.reviver);
    else creds = initAuthCreds();
    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    for (const id of ids) {
                        const doc = await Auth.findById(`${type}-${id}`);
                        let val = doc ? JSON.parse(JSON.stringify(doc.data), BufferJSON.reviver) : undefined;
                        if (type === 'app-state-sync-key' && val)
                            val = proto.Message.AppStateSyncKeyData.fromObject(val);
                        data[id] = val;
                    }
                    return data;
                },
                set: async (data) => {
                    const ops = [];
                    for (const [type, ids] of Object.entries(data))
                        for (const [id, val] of Object.entries(ids || {})) {
                            const _id = `${type}-${id}`;
                            ops.push(val
                                ? Auth.findByIdAndUpdate(_id, { data: JSON.parse(JSON.stringify(val, BufferJSON.replacer)) }, { upsert: true })
                                : Auth.findByIdAndDelete(_id));
                        }
                    await Promise.all(ops);
                }
            }
        },
        saveCreds: async () => {
            await Auth.findByIdAndUpdate('creds',
                { data: JSON.parse(JSON.stringify(creds, BufferJSON.replacer)) },
                { upsert: true });
        }
    };
}

// ─────────────────────────────────────────────
// ANTI-FLOOD — 10 mensajes iguales en < 5s → sanción + cierre
// ─────────────────────────────────────────────
const floodMap    = new Map(); // { text, times[] } por usuario — mensajes de texto iguales
const stickerMap  = new Map(); // times[] por usuario — stickers sin importar cuál sea
const allMsgMap   = new Map(); // times[] por usuario — cualquier mensaje (flood general)
// Acciones promote/demote iniciadas por el bot vía comando — el event handler las ignora
const pendingBotActions = new Set();
function markBotAction(gid, uid, act) {
    const key = `${gid}:${uid}:${act}`;
    pendingBotActions.add(key);
    setTimeout(() => pendingBotActions.delete(key), 8000);
}
const spamMsgs    = new Map(); // keys de mensajes para borrar
const cooldownMap = new Map();
const sancionCooldown = new Map();

const MAX_ADV = 3;

// Registra la key del mensaje para poder borrarlo después
function trackMsg(userId, key) {
    const arr = spamMsgs.get(userId) || [];
    arr.push(key);
    if (arr.length > 50) arr.shift();
    spamMsgs.set(userId, arr);
}

// Devuelve true si el usuario mandó 10+ mensajes con el mismo texto en < 5s
function checkFlood(userId, text) {
    const now = Date.now();
    const d = floodMap.get(userId) || { t: '', times: [] };
    const times = d.t === text
        ? d.times.filter(t => now - t < 5000)
        : [];
    times.push(now);
    floodMap.set(userId, { t: text, times });
    return times.length >= 10;
}

// Devuelve true si el usuario mandó 10+ stickers en < 5s (sin importar cuáles)
function checkStickerFlood(userId) {
    const now = Date.now();
    const times = (stickerMap.get(userId) || []).filter(t => now - t < 5000);
    times.push(now);
    stickerMap.set(userId, times);
    return times.length >= 10;
}

// Devuelve true si el usuario mandó 15+ mensajes (cualquier tipo) en < 10s
function checkGeneralFlood(userId) {
    const now = Date.now();
    const times = (allMsgMap.get(userId) || []).filter(t => now - t < 10000);
    times.push(now);
    allMsgMap.set(userId, times);
    return times.length >= 15;
}

function isOnCooldown(userId) {
    const last = cooldownMap.get(userId) || 0;
    if (Date.now() - last < 10000) return true;
    cooldownMap.set(userId, Date.now());
    return false;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function timeAgo(date) {
    const d = Date.now() - new Date(date).getTime();
    const m = Math.floor(d/60000);
    if (m < 60) return `${m}m`;
    const h = Math.floor(m/60);
    if (h < 24) return `${h}h`;
    return `${Math.floor(h/24)}d`;
}

const PORN_DOMAINS = [
    // Hetero
    'pornhub','xvideos','xnxx','xhamster','redtube','youporn','tube8',
    'spankbang','eporner','hclips','txxx','fuq','porntrex','4tube',
    'fapvid','beeg','porndoe','brazzers','bangbros','mofos','realitykings',
    'naughtyamerica','digitalplayground','wankz','porn.com','sex.com',
    'tnaflix','drtuber','empflix','porndig','pervclips','pornone',
    'sexvid','vivatube','porn300','pornoxo','cliphunter','ashemaletube',
    // Gay / LGBTQ+
    'gaymaletube','gaytube','gayforit','manporn','dudeporn','men.com',
    'nextdoormale','corbin fisher','sean cody','active duty','randy blue',
    'boyfun','gayporn','grindr','scruff','growlr','jackd',
    // Cams / fans
    'onlyfans','fansly','stripchat','chaturbate','bongacams','cam4',
    'myfreecams','livejasmin','camsoda','streamate','jasmin','flirt4free',
    'camdude','gaystreams','camboys','cam4gay',
    // Escorts / contenido oculto
    'xbiz','adultfriendfinder','ashley madison','seeking','sugarbook',
    'eros.com','slixa','tryst','switter','skipthegames','listcrawler',
    'adultsearch','bedpage','backpage','humaniplex','fetlife',
    // Hentai / anime
    'hentai','nhentai','fakku','rule34','gelbooru','danbooru','sankakucomplex',
    'e-hentai','exhentai','luscious','nozomi',
    // Otros
    'porntraffic','trafficjunky','nudevista','theync','kashtanka',
    'motherless','xbabe','fux','sex.tube','sextvx','porntube'
];

function containsPornLink(text) {
    if (!text) return false;
    const lower = text.toLowerCase();
    return PORN_DOMAINS.some(d => lower.includes(d));
}

function containsExternalLink(text) {
    if (!text) return false;
    return /chat\.whatsapp\.com\/[a-zA-Z0-9]+/i.test(text);
}

// Extrae los códigos de invitación de un texto
function extractInviteCodes(text) {
    const matches = [...(text||'').matchAll(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/gi)];
    return matches.map(m => m[1]);
}

// Normaliza nombre — impenetrable contra duplicados por caps, tildes, leetspeak, símbolos, números
const LEET = {'0':'o','1':'i','3':'e','4':'a','5':'s','6':'g','7':'t','8':'b','9':'g',
              '@':'a','$':'s','!':'i','|':'i','(':'c','+':'t'};
function normalizarNombre(n) {
    if (!n) return '';
    return n.toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quita tildes
        .split('').map(c => LEET[c] || c).join('')         // reemplaza leetspeak
        .replace(/[^a-z0-9]/g, '')                          // quita todo lo que no sea letra o número
        .trim();
}

async function labelFor(communityId, jid) {
    if (!jid) return 'Sistema';
    try { const u = await User.findOne({ groupId: communityId, userId: jid }); if (u?.personaje) return u.personaje; } catch(_) {}
    return 'Miembro';
}

async function logAction(groupId, accion, by, target = '') {
    try { await BotLog.create({ groupId, accion, by, target }); } catch (_) {}
}

// Extrae número limpio desde JID (ej: "5491112223333@s.whatsapp.net" → "5491112223333")
function numFromJid(jid = '') {
    return jid.replace(/@.+/, '').replace(/:\d+$/, '');
}

// ─────────────────────────────────────────────
// OWNER SYSTEM — MongoDB, máx 2, contraseña
// ─────────────────────────────────────────────
// FIX M-5: Sin default visible en código — fuerza definición explícita en .env
const OWNER_PASSWORD = process.env.OWNER_PASS;
if (!OWNER_PASSWORD) {
    console.error('· ⚠️ OWNER_PASS no configurado en .env — el comando !claim estará deshabilitado hasta configurarlo.');
}
let ownerCache = new Set();

async function loadOwners() {
    try {
        const docs = await Owner.find({});
        ownerCache = new Set(docs.map(o => o.userId));
        return ownerCache;
    } catch (_) { return ownerCache; }
}

async function isOwner(jid) {
    const num = numFromJid(jid);
    if (ownerCache.has(num)) return true;
    try {
        const doc = await Owner.findOne({ userId: num });
        if (doc) { ownerCache.add(num); return true; }
    } catch (_) {}
    return false;
}

async function addOwner(jid) {
    const num = numFromJid(jid);
    try {
        const count = await Owner.countDocuments();
        if (count >= 2) return false;
        await Owner.findOneAndUpdate({ userId: num }, { userId: num }, { upsert: true });
        ownerCache.add(num);
        return true;
    } catch (_) { return false; }
}

async function removeOwner(jid) {
    const num = numFromJid(jid);
    try {
        await Owner.deleteOne({ userId: num });
        ownerCache.delete(num);
    } catch (_) {}
}

async function isGroupOwner(meta, jid) {
    return await isOwner(jid);
}

// queryNudeNet, analizarResultadoNudeNet, isExplicitOrGore — definidas arriba (bloque SAFETY)

// Obtiene target desde mención O desde mensaje citado
function getTargetFromMsg(message) {
    // 1. Mención directa
    const mentions = message.message?.extendedTextMessage?.contextInfo?.mentionedJid || [];
    if (mentions.length) return mentions[0];
    // 2. Mensaje citado (respondiendo)
    const quoted = message.message?.extendedTextMessage?.contextInfo?.participant
        || message.message?.extendedTextMessage?.contextInfo?.remoteJid;
    if (quoted && quoted.includes('@')) return quoted;
    return null;
}
function getMentions(message) {
    return message.message?.extendedTextMessage?.contextInfo?.mentionedJid || [];
}

// ─────────────────────────────────────────────
// MENÚS
// ─────────────────────────────────────────────
const MENU_ACCIONES = `
‎ ‎ ‎ ‎ ‎ ━━━━━━━━ ꒰ ᧔🎭᧓ ꒱ ━━━━━━━━
        ⤹ ⊹ ୨୧ 𝗔𝗖𝗖𝗜𝗢𝗡𝗘𝗦 𝗠𝗘𝗡𝗨 ⿻ ₊˚๑
     ━━━━━━━━━━━━━━━━━━━━━━━

                     𝄄 𓈒   ⁺ 𝓒ariño   𓏼

       𝄄➥𓈒   ⁺ 💋  *!besar @usuario*
       𝄄   _Manda un beso a alguien._

       𝄄➥𓈒   ⁺ 🤗  *!hug @usuario*
       𝄄   _Dale un abrazo a alguien._

                     𝄄 𓈒   ⁺ 𝓤tilidades   𓏼

       𝄄➥𓈒   ⁺ 🎵  *!v2a*
       𝄄   _Responde a un video para extraer el audio como nota de voz._

       𝄄➥𓈒   ⁺ 🖼️  *!s*
       𝄄   _Responde a una imagen o GIF para convertirla en sticker._

‎ ‎ ‎ ‎ ‎ ━━━━━━━━━━━━━━━━━━━━━━`.trim();

const MENU_GENERAL = `
‎ ‎ ‎ ‎ ‎ ━━━━━━━━ ꒰ ᧔🌳᧓ ꒱ ━━━━━━━━
        ⤹ ⊹ ୨୧ 𝗚𝗘𝗡𝗘𝗥𝗔𝗟 𝗠𝗘𝗡𝗨 ⿻ ₊˚๑
     ━━━━━━━━━━━━━━━━━━━━━━━

                     𝄄 𓈒   ⁺ 𝓟ersonajes   𓏼

       𝄄➥𓈒   ⁺ 📃  *!lista*
       𝄄   _Muesta la lista de personajes ocupados, buscados y disponibles recientemente._

       𝄄➥𓈒   ⁺ 🎭  *!cambio*
       𝄄   _Pide a los admins un cambio de personaje, requiere permiso._

       𝄄➥𓈒   ⁺ 📂  *!persoinfo*
       𝄄   _Poniendo el nombre del personaje, muestra su información sin etiquetar a la persona._

       𝄄➥𓈒   ⁺ 🌷  *!disponibles*
       𝄄   _Muestra la lista de personajes que recientemente han sido liberados._

       𝄄➥𓈒   ⁺ 🔍  *!buscados*
       𝄄   _Muestra la lista de personajes buscados por los miembros del team._

       𝄄➥𓈒   ⁺ 🔎  *!buscar*
       𝄄   _Añade el nombre de un personaje para ver si está disponible de una manera más rapida._

       𝄄➥𓈒   ⁺ 📦  *!pedir*
       𝄄   _Añade un personaje a la lista de buscados añadiendo el nombre del personaje._

                     𝄄 𓈒   ⁺ 𝓟erfil   𓏼

       𝄄➥𓈒   ⁺ 👥  *!info*
       𝄄   _Muestra tu info, si respondes un mensaje o etiquetas a alguien, muestra la de los demás._

       𝄄➥𓈒   ⁺ 📊  *!top10*
       𝄄   _Muestra a los que más mensajes enviaron en la comunidad._

       𝄄➥𓈒   ⁺ 📜  *!reglas*
       𝄄   _Muestra las reglas de la comunidad. Recuerda cumplirlas._

                     𝄄 𓈒   ⁺ 𝓘nactividad   𓏼

       𝄄➥𓈒   ⁺ ✍️  *!excusa*
       𝄄   _Pon una excusa de tu inactividad, ejemplo: !excusa semana de examenes [días]d._

       𝄄➥𓈒   ⁺ 🗞️  *!excusa off*
       𝄄   _Quita una excusa de la lista._

       𝄄➥𓈒   ⁺ 📉  *!ver excusa*
       𝄄   _Muestra las excusas de todos o de alguien específico._

                     𝄄 𓈒   ⁺ 𝓡elaciones   𓏼

       𝄄➥𓈒   ⁺ 💍  *!mary / !casar*
       𝄄   _Cásate con uno o más usuarios, etiquetando o poniendo su personaje._

       𝄄➥𓈒   ⁺ ✔️  *!aceptar*
       𝄄   _Acepta una propuesta de pareja._

       𝄄➥𓈒   ⁺ ❌  *!rechazar*
       𝄄   _Rechaza una propuesta de pareja._

       𝄄➥𓈒   ⁺ 💔  *!divorcio*
       𝄄   _Termina una relación._

       𝄄➥𓈒   ⁺ 💑  *!parejas*
       𝄄   _Muestra las parejas y poliamores de la comunidad._

       𝄄➥𓈒   ⁺ 💑  *!mi pareja*
       𝄄   _Muestra tu(s) pareja(s)._

                     𝄄 𓈒   ⁺ 𝓐porte   𓏼

       𝄄➥𓈒   ⁺ 🌐  *!grupo*
       𝄄   _Muestra las estadísticas del grupo._

       𝄄➥𓈒   ⁺ 🚨  *!reporte*
       𝄄   _Reporta algo a los admins._

       𝄄➥𓈒   ⁺ 💌  *!sugerencia*
       𝄄   _Añade una sugerencia a los admins._

‎ ‎ ‎ ‎ ‎ ━━━━━━━━━⊱⋆⊰━━━━━━━━━━
‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎   ‎ ˖ ࣪ ꉂ˙STAFF MENUˎˊ-
  ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ *!smenu*

‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎   ‎ ˖ ࣪ ꉂ˙OWNER MENUˎˊ-
  ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ *!pmenu*
‎ ‎ ‎ ‎ ‎ ━━━━━━━━━━━━━━━━━━━━━━`.trim();

const MENU_STAFF = `
‎ ‎ ‎ ‎ ‎ ━━━━━━━━ ꒰ ᧔🌊᧓ ꒱ ━━━━━━━━
           ⤹ ⊹ ୨୧ 𝗦𝗧𝗔𝗙𝗙 𝗠𝗘𝗡𝗨 ⿻ ₊˚๑
     ━━━━━━━━━━━━━━━━━━━━━━━


                     𝄄 𓈒   ⁺ 𝓐dvertencias   𓏼

       𝄄➥𓈒   ⁺ ⚠️  *!adv*
       𝄄   _Etiqueta o responde a alguien y pon el motivo de tu advertencia._

       𝄄➥𓈒   ⁺  🏅  *!quitar adv*
       𝄄   _Le quita una advertencia a la persona que etiquetes o respondas._

       𝄄➥𓈒   ⁺ 📃  *!ver advs*
       𝄄   _Muestra las advertencias de todos, o la de alguien en específico si lo etiquetas o respondes._

       𝄄➥𓈒   ⁺ ♻️  *!resets advs*
       𝄄   _Le quita *todas* las advertencias a la persona que respondas o etiquetes._

                      𝄄 𓈒   ⁺ 𝓟ersonajes   𓏼

       𝄄➥𓈒   ⁺  🎭  *!perso*
       𝄄   _Asignación directa de personaje a un usuario._

       𝄄➥𓈒   ⁺  ✅  *!aceptar cambio*
       𝄄   _Confirma el cambio de personaje solicitado._

       𝄄➥𓈒   ⁺  ❎  *!negar cambio*
       𝄄   _Rechaza la solicitud de cambio de personaje._

       𝄄➥𓈒   ⁺  🔄  *!cambiarperso*
       𝄄   _Cambia el nombre del personaje actual._

       𝄄➥𓈒   ⁺  🗑️  *!resetperso*
       𝄄   _Elimina el personaje asignado a alguien._

       𝄄➥𓈒   ⁺  🔀  *!moverperso*
       𝄄   _Pasa un personaje de un usuario a otro._

       𝄄➥𓈒   ⁺  📋  *!lista2*
       𝄄   _Muestra la lista con etiquetas de actividad._

       𝄄➥𓈒   ⁺  ⭐  *!viplist*
       𝄄   _Muestra a los miembros marcados como VIP._

       𝄄➥𓈒   ⁺  🚩  *!risklist*
       𝄄   _Muestra a los veteranos marcados con riesgo (flag)._ 

       𝄄➥𓈒   ⁺  👤  *!sinperso*
       𝄄   _Lista de personas que no tienen personaje._

       𝄄➥𓈒   ⁺  🗑️  *!quitardisponible*
       𝄄   _Quita un nombre de la lista de disponibles._

       𝄄➥𓈒   ⁺  🗑️  *!limpiardisp*
       𝄄   _Borra todos los personajes de la lista de disponibles._

                     𝄄 𓈒   ⁺ 𝓜oderación   𓏼

       𝄄➥𓈒   ⁺  ☠️  *!eli*
       𝄄   _Saca al usuario del grupo actual._

       𝄄➥𓈒   ⁺  🔨  *!ban*
       𝄄   _Baneo total de todos los grupos de la comunidad._

       𝄄➥𓈒   ⁺  🌙  *!inactivos / !activos*
       𝄄   _Muestra el estado de actividad de los miembros._

       𝄄➥𓈒   ⁺  🔧  *!fixperso*
       𝄄   _Repara errores en el registro de personajes._

       𝄄➥𓈒   ⁺  🔍  *!check*
       𝄄   _Analiza si un archivo contiene nsfw o gore._

       𝄄➥𓈒   ⁺  🔗  *!antilink on/off*
       𝄄   _Activa o desactiva el bloqueo de enlaces._

       𝄄➥𓈒   ⁺  🚪  *!autoclose on/off*
       𝄄   _Configura el cierre automático del grupo._

       𝄄➥𓈒   ⁺  🔞  *!autoban NSFW on/off*
       𝄄   _Activa o desactiva el que el bot elimine a alguien que mandó contenido +18._

       𝄄➥𓈒   ⁺  🔪  *!autoban Gore on/off*
       𝄄   _Activa o desactiva el que el bot elimine a alguien que mandó contenido violento._

       𝄄➥𓈒   ⁺  🔔  *!autoban ADV on/off*
       𝄄   _Activa o desactiva el que el bot elimine a alguien que llegó a las advertencias máximas._

       𝄄➥𓈒   ⁺  🔐  *!reservar / !desreservar*
       𝄄   _Gestiona la reserva de nombres de personajes._

                     𝄄 𓈒   ⁺ 𝓖rupo   𓏼

       𝄄➥𓈒   ⁺  🔓  *!abrir / !cerrar*
       𝄄   _Permite o restringe la escritura en el chat._

       𝄄➥𓈒   ⁺  ↕️  *!promover / !degradar*
       𝄄   _Le quita o le da admin a un usuario._

       𝄄➥𓈒   ⁺  📜  *!set reglas*
       𝄄   _Establece las reglas del grupo._

       𝄄 𓈒   ⁺ 𝓡egistros   𓏼

       𝄄➥𓈒   ⁺  📌  *!nota*
       𝄄   _Añade una anotación al perfil de un usuario._

       𝄄➥𓈒   ⁺  🗂️  *!historial*
       𝄄   _Muestra el registro de acciones de alguien._

       𝄄➥𓈒   ⁺  🏓  *!ping*
       𝄄   _Verifica si el bot está activo._

✦ ══════════════════ ✦`.trim();

const MENU_OWNER = `
    ✧ ╔═══ ༺ 👑 ༻ ═══╗ ✧
            O W N E R 
    ✧ ╚═══ ༺ 👑 ༻ ═══╝ ✧

         𝄄➥𓈒   ⁺  👥  *!owners*
         𝄄➥𓈒   ⁺ 👤   *!quitar owner*
         𝄄➥𓈒   ⁺ 🏓   *!ownerping*
         𝄄➥𓈒   ⁺ 🔬   *!testear*
         𝄄➥𓈒   ⁺ 🏠   *!set principal*
         𝄄➥𓈒   ⁺ 🏠   *!set secundaria*
         𝄄➥𓈒   ⁺ 🗑️   *!nuke*
         𝄄➥𓈒   ⁺ 💤   *!beyonder on/off*
  
         𝄄➥𓈒   ⁺ 🔒   *!lock [funcion] on/off*
                           *Funciones*
         · Antilink ◌ ִ ੭ ˑ    · Autoclose 𐀔࣭ ៸៸۫
         · Antinsfw 𑁯͟ ɞ    · Antiflood ᭡ ˎˊ-⁣
         · Lockperso ִ✧   · Autoban ۫ ͟ଓ ˑ𔒱࣭

    ✦ ════════════════ ✦`.trim();


// obtenerRespuestaIA, simulateTyping, humanDelay — definidas arriba (bloque IA)

/**
 * Extrae de MongoDB los datos de contexto necesarios para pasar a obtenerRespuestaIA.
 * Reemplaza getCerebroData + buildOllamaSystemPrompt (ahora el prompt se construye dentro de obtenerRespuestaIA).
 */
async function getContextoIA(communityId, userId, meta = null, groupPersonality = 'default') {
    const num = numFromJid(userId);
    try {
        const [mem, gm, userDoc, cfg] = await Promise.all([
            BeyondMemory.findOne({ communityId, userId }).lean(),
            GlobalUserMemory.findOne({ userId: num }).lean(),
            User.findOne({ groupId: communityId, userId }).lean(),
            meta ? null : null, // meta se pasa directamente
        ]);
        return {
            nombreUsuario:  gm?.nombreReal || userDoc?.personaje || ('@' + num),
            sentimiento:    mem?.sentimiento ?? gm?.sentimientoGlobal ?? 0,
            vinculo:        mem?.vinculoSocial || mem?.vinculo || gm?.vinculoGlobal || 'neutral',
            resumen:        mem?.resumenPersonalidad || null,
            grupoNombre:    meta?.subject || 'el grupo',
            personalidad:   groupPersonality || 'default',
        };
    } catch (_) {
        return { nombreUsuario: '@' + num, sentimiento: 0, vinculo: 'neutral', resumen: null, grupoNombre: 'el grupo', personalidad: 'default' };
    }
}

/**
 * Guarda un resumen de la conversación en BeyondMemory para que el bot
 * "recuerde" datos clave de la persona en futuros mensajes.
 * Usa obtenerRespuestaIA en modo resumen — sin afectar la respuesta al usuario.
 */
async function updateResumenPersonalidad(communityId, userId, userMsg, botReply) {
    try {
        const nuevoDato = await obtenerRespuestaIA(
            `Genera UNA sola línea (máx 15 palabras) resumiendo un dato útil sobre esta persona para recordarla. Último mensaje: "${(userMsg||'').slice(0,150)}". Mi respuesta: "${(botReply||'').slice(0,150)}". Responde SOLO esa línea.`,
            'sistema',
            {} // sin contexto de vínculo — es una llamada interna de memoria
        );
        if (!nuevoDato || nuevoDato.length > 300) return;
        const mem  = await BeyondMemory.findOne({ communityId, userId }).lean();
        const prev = (mem?.resumenPersonalidad || '').split('\n').filter(Boolean).slice(-4);
        await BeyondMemory.findOneAndUpdate(
            { communityId, userId },
            { $set: { lastOrganic: new Date(), resumenPersonalidad: [...prev, nuevoDato].join('\n') } },
            { upsert: true, setDefaultsOnInsert: true }
        );
    } catch (_) {}
}

// ─────────────────────────────────────────────
// CEREBRO: helpers de BeyondMemory
// ─────────────────────────────────────────────

// IDs conocidos con roles fijos — se puede expandir vía .env
// Formato: número limpio sin @s.whatsapp.net
const BEYOND_OWNERS_NUMS  = (process.env.BEYOND_OWNERS  || '').split(',').filter(Boolean); // Carlos Yoxel, Renata
const BEYOND_ADMINS_NUMS  = (process.env.BEYOND_ADMINS  || '').split(',').filter(Boolean); // Rei, Milena, Lara, Kiara

// ─────────────────────────────────────────────
// LISTA BLANCA DE DMS — solo estos JIDs reciben respuesta en privado.
// Formato: 'CODIGO_PAIS + NUMERO@s.whatsapp.net'
// Agrega aquí tu número y el de tus admins. Los demás son ignorados.
// ─────────────────────────────────────────────
// Número del owner para el filtro Anti-Mamá — SOLO este JID puede hablar en privado
// Ponlo directamente o cárgalo desde .env (BEYOND_OWNERS toma el primero)
// FIX L-2: evitar que OWNER_JID quede como '@s.whatsapp.net' si BEYOND_OWNERS está vacío
const _ownerNumRaw = process.env.OWNER_JID
    ? process.env.OWNER_JID.replace('@s.whatsapp.net', '').trim()
    : (process.env.BEYOND_OWNERS || '').split(',')[0]?.trim();
const OWNER_JID = _ownerNumRaw ? `${_ownerNumRaw}@s.whatsapp.net` : null;
if (!OWNER_JID) console.warn('· ⚠️ OWNER_JID no configurado — DMs del owner desactivados');

// ─────────────────────────────────────────────
// WHITELIST DE DMS — pon aquí tu número y los de tus admins.
// Formato: 'CODIGO_PAIS + NUMERO@s.whatsapp.net'
// Si el remitente NO está en esta lista, el mensaje se ignora COMPLETAMENTE.
// ─────────────────────────────────────────────
const whitelist = [
    // Pon tus números aquí directamente:
    // 'TU_NUMERO@s.whatsapp.net',
    // 'NUMERO_ADMIN@s.whatsapp.net',
    // O cárgalos desde .env (BEYOND_OWNERS, BEYOND_ADMINS, DM_EXTRA_USERS):
    ...(process.env.BEYOND_OWNERS  || '').split(',').filter(Boolean).map(n => n.trim() + '@s.whatsapp.net'),
    ...(process.env.BEYOND_ADMINS  || '').split(',').filter(Boolean).map(n => n.trim() + '@s.whatsapp.net'),
    ...(process.env.DM_EXTRA_USERS || '').split(',').filter(Boolean).map(n => n.trim() + '@s.whatsapp.net'),
];
// Set para lookup O(1) — no tocar
const AUTHORIZED_DM_NUMS = new Set(whitelist.map(j => j.replace('@s.whatsapp.net', '')));

async function getBeyondMemory(communityId, userId) {
    const num = numFromJid(userId);
    try {
        let mem = await BeyondMemory.findOne({ communityId, userId });
        if (!mem) {
            // Determinar vínculo inicial
            let vinculo = 'neutral';
            if (BEYOND_OWNERS_NUMS.includes(num)) vinculo = 'owner';
            else if (BEYOND_ADMINS_NUMS.includes(num)) vinculo = 'admin';
            mem = await BeyondMemory.create({ communityId, userId, vinculo });
        }
        return mem;
    } catch(_) { return { vinculo: 'neutral', sentimiento: 0, tono: [] }; }
}

// ─────────────────────────────────────────────
// MEMORIA GLOBAL — helpers de GlobalUserMemory
// ─────────────────────────────────────────────
const globalMemCache   = new Map(); // userId → { data, ts }
const GLOBAL_MEM_TTL   = 5 * 60 * 1000; // 5 minutos
// FIX A-5: limpiar entradas expiradas cada 10 minutos para evitar memory leak
setInterval(() => {
    const now = Date.now();
    for (const [key, val] of globalMemCache) {
        if (now - val.ts > GLOBAL_MEM_TTL) globalMemCache.delete(key);
    }
}, 10 * 60 * 1000);

async function getGlobalMemory(userId) {
    const num = numFromJid(userId);
    const cached = globalMemCache.get(num);
    if (cached && Date.now() - cached.ts < GLOBAL_MEM_TTL) return cached.data;
    try {
        let gm = await GlobalUserMemory.findOne({ userId: num }).lean();
        if (!gm) {
            let vinculoGlobal = 'neutral';
            if (BEYOND_OWNERS_NUMS.includes(num))  vinculoGlobal = 'owner';
            else if (BEYOND_ADMINS_NUMS.includes(num)) vinculoGlobal = 'admin';
            gm = await GlobalUserMemory.create({ userId: num, vinculoGlobal });
            gm = gm.toObject();
        }
        globalMemCache.set(num, { data: gm, ts: Date.now() });
        return gm;
    } catch(_) { return { userId: num, vinculoGlobal: 'neutral', sentimientoGlobal: 0, gruposProblemáticos: [], gruposActivos: [], tags: [] }; }
}

async function updateGlobalMemory(userId, communityId, updates) {
    const num = numFromJid(userId);
    globalMemCache.delete(num);
    try {
        // Separar operadores Mongo ($inc, $addToSet...) de campos planos
        const mongoOps   = {};
        const plainFields = { lastSeenGlobal: new Date() };
        for (const [k, v] of Object.entries(updates || {})) {
            if (k.startsWith('$')) mongoOps[k] = v;
            else plainFields[k] = v;
        }
        if (communityId) {
            mongoOps['$addToSet'] = { ...(mongoOps['$addToSet'] || {}), gruposActivos: communityId };
        }
        await GlobalUserMemory.findOneAndUpdate(
            { userId: num },
            { $set: plainFields, ...mongoOps },
            { upsert: true, setDefaultsOnInsert: true }
        );
    } catch(_) {}
}

// Construye el bloque de contexto global para inyectar en el system prompt
function buildGlobalContext(gm, communityId) {
    if (!gm) return '';
    const lines = [];
    const { vinculoGlobal, sentimientoGlobal, gruposProblemáticos, gruposActivos, nombreReal, tags } = gm;

    if (vinculoGlobal === 'owner')
        lines.push('⚡ OWNER GLOBAL: Este usuario es uno de tus Creadores. Respeto real y genuino en CUALQUIER grupo.');
    else if (vinculoGlobal === 'admin')
        lines.push('🛡️ ADMIN GLOBAL: Staff de confianza conocido en todos los grupos.');
    else if (vinculoGlobal === 'palomo') {
        const otros = (gruposProblemáticos || []).filter(g => g !== communityId);
        if (otros.length)
            lines.push('⚠️ MEMORIA CRUZADA: Este usuario ya jodió en ' + otros.length + ' grupo(s) antes. Podés decirle "Mira quién apareció... el mismo loco del otro grupo. No creas que porque te mudaste de chat se me olvidó lo que hiciste."');
        else
            lines.push('⚠️ Usuario marcado como "palomo". Respondele con menos paciencia.');
    }

    if (nombreReal) lines.push('📛 Se llama o le dicen: ' + nombreReal);
    if (sentimientoGlobal >= 10)  lines.push('💚 Historial global positivo: siempre buena onda con Beyond.');
    else if (sentimientoGlobal <= -8) lines.push('💢 Historial global negativo: suele molestar en distintos grupos.');
    if (tags?.includes('spammer'))  lines.push('🔁 Tag: spammer — historial de flood en otros grupos.');
    if (tags?.includes('flirty'))   lines.push('😏 Tag: flirty — le gusta coquetear, podés seguirle el juego.');
    if ((gruposActivos || []).length > 1) lines.push('🌐 Activo en ' + gruposActivos.length + ' grupos donde está Beyond.');

    return lines.length ? '\nCONTEXTO GLOBAL DEL USUARIO:\n' + lines.join('\n') : '';
}

// Actualizar sentimiento y evolucionar el vínculo social segun acumulado
async function updateSentimiento(communityId, userId, delta) {
    try {
        const mem = await BeyondMemory.findOneAndUpdate(
            { communityId, userId },
            { $inc: { sentimiento: delta } },
            { upsert: true, setDefaultsOnInsert: true, returnDocument: 'after' }
        );
        if (!mem) return;

        const s = mem.sentimiento || 0;
        const vs = mem.vinculoSocial;

        // Progresion organica del vinculo:
        // Nunca salta de neutro a pareja — la pareja es solo por declaracion explicita
        // La amistad y enemistad se construyen con el tiempo
        let newVinculo = vs;
        if (!vs || vs === 'neutral') {
            if (s >= 12) newVinculo = 'amigo';
            else if (s <= -8) newVinculo = 'enemigo';
        } else if (vs === 'amigo' && s <= -5) {
            newVinculo = 'enemigo'; // la amistad se rompe si acumula suficiente conflicto
        } else if (vs === 'enemigo' && s >= 10) {
            newVinculo = 'amigo';  // reconciliacion posible
        }

        if (newVinculo !== vs) {
            await BeyondMemory.findOneAndUpdate(
                { communityId, userId },
                { $set: { vinculoSocial: newVinculo } }
            );
            console.log(`· [Beyond] vinculoSocial ${vs} → ${newVinculo} para ${userId}`);
        }
    } catch(_) {}
}

// Detecta si el mensaje contiene insultos dirigidos al bot
function esInsultoAlBot(texto) {
    const lower = (texto || '').toLowerCase();
    const INSULTOS = ['idiota','estupido','estúpido','inutil','inútil','maldito','hdp','qliao','cállate','callate','molesto','pesado','animal','bestia','imbécil','imbecil'];
    return INSULTOS.some(i => lower.includes(i));
}
function esTratoBueno(texto) {
    const lower = (texto || '').toLowerCase();
    const BUENOS = ['gracias','excelente','genial','crack','eres el mejor','te quiero','lo mejor','perfecto','increíble','increible','buenísimo','buenisimo'];
    return BUENOS.some(b => lower.includes(b));
}

// primerNombreStatic — helper de nombres
function primerNombreStatic(p) {
    if (!p) return p;
    return p.split(/[\s\/]/)[0].trim();
}

// Detecta si dos nombres de personajes son el mismo (typos/alias) usando la IA
async function detectarDuplicadoDisponible(nombreNuevo, nombresExistentes) {
    if (!nombresExistentes.length) return null;
    try {
        const pregunta =
            `¿El personaje "${nombreNuevo}" es el MISMO que alguno de estos (considera errores de ortografía y alias)?\n` +
            nombresExistentes.map((n, i) => `${i + 1}. "${n}"`).join('\n') +
            `\nResponde SOLO con el número que coincide, o "0" si ninguno.`;
        const text = await obtenerRespuestaIA(pregunta, 'sistema', {});
        const idx = parseInt(text) - 1;
        if (!isNaN(idx) && idx >= 0 && idx < nombresExistentes.length) return nombresExistentes[idx];
    } catch(e) { console.error('· IA perso:', e.message); }
    return null;
}

// ─────────────────────────────────────────────
// HELPER: detectar si el bot es admin en un grupo
// Soporta JIDs normales Y formato LID (IDs largos de comunidades WhatsApp)
// En grupos de comunidades, participants usan IDs tipo "105308873593051" (LID)
// en lugar del número de teléfono. Baileys expone sock.user.lid para comparar.
// ─────────────────────────────────────────────
function isBotAdmin(meta, sock) {
    const parts = meta?.participants || [];
    if (!parts.length) return false;

    const clean = jid => (jid || '').replace(/@.*/, '').replace(/:.*/, '');

    // Método 1: comparar por número limpio (JID normal)
    const myNum = clean(BOT_NUM || sock?.user?.id || '');

    // Método 2: comparar por LID (grupos de comunidades multi-device)
    // sock.user?.lid tiene el formato "XXXXXXXXX:0@lid"
    const myLid = clean(sock?.user?.lid || '');

    const found = parts.find(p => {
        const pid = clean(p.id);
        const isAdmin = p.admin === 'admin' || p.admin === 'superadmin';
        if (!isAdmin) return false;
        // Comparar contra número normal O contra LID
        return (myNum && pid === myNum) || (myLid && pid === myLid);
    });

    return !!found;
}

// ─────────────────────────────────────────────
// BOT
// ─────────────────────────────────────────────
let BOT_NUM = ''; // número del bot, se llena al conectarse
let _pairingRequested = false; // FIX M-2: ex global._pairingRequested
let _retryCount = 0;
const MAX_RETRIES = 8;

async function startBot() {
    if (mongoose.connection.readyState !== 1)
        await new Promise(r => mongoose.connection.once('connected', r));
    await loadOwners();
    console.log(`· owners cargados: ${ownerCache.size > 0 ? [...ownerCache].join(', ') : 'ninguno'}`);

    const { state, saveCreds } = await useMongoDBAuthState();
    const { version } = await fetchLatestBaileysVersion();

    const PHONE_NUMBER = process.env.BOT_PHONE || null; // ej: 18494486613

    const sock = makeWASocket({
        version, auth: state,
        logger: pino({ level: 'silent' }),
        defaultQueryTimeoutMs: 60000,
        printQRInTerminal: false,
    });

    sock.ev.on('creds.update', saveCreds);

    // Pairing code si hay BOT_PHONE en .env, sino QR
    if (PHONE_NUMBER && !sock.authState.creds.registered && !_pairingRequested) {
        _pairingRequested = true;
        setTimeout(async () => {
            try {
                const code = await sock.requestPairingCode(PHONE_NUMBER.replace(/[^0-9]/g, ''));
                process.stdout.write('\x1Bc');
                console.log('\n\n  ╔══════════════════════════════╗');
                console.log('  ║   CÓDIGO DE VINCULACIÓN      ║');
                console.log('  ╚══════════════════════════════╝\n');
                console.log(`        👉  ${code}  👈\n`);
                console.log('  1. WhatsApp → Dispositivos vinculados');
                console.log('  2. Vincular con número de teléfono');
                console.log('  3. Ingresa el número del bot');
                console.log('  4. Ingresa este código\n');
            } catch (e) {
                _pairingRequested = false;
                console.error('· pairing code error:', e.message);
            }
        }, 5000);
    }

    sock.ev.on('connection.update', async ({ qr, connection, lastDisconnect }) => {
        if (qr && !PHONE_NUMBER) {
            const qrcode = require('qrcode-terminal');
            process.stdout.write('\x1Bc');
            console.log('\n\n  ── Escanea este QR con WhatsApp ──\n');
            qrcode.generate(qr, { small: true });
            console.log('\n  ── WhatsApp → Dispositivos vinculados ──\n');
        }
        if (connection === 'open') {
            BOT_NUM = (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
            _retryCount = 0;
            console.log('· beyonder online ✓ | bot num:', BOT_NUM);
            // El Space de Phi-3 despierta automáticamente en la primera petición — no hay pull manual

            // ── Anuncio de actualización tras deploy ──
            if (_deployAnnouncePending) {
                _deployAnnouncePending = false;
                setTimeout(async () => {
                    try {
                        // Buscar grupo principal para anunciar
                        const todos = await sock.groupFetchAllParticipating().catch(() => ({}));
                        let targetGid = null;
                        for (const [gid] of Object.entries(todos)) {
                            const cfgD = await Config.findOne({ groupId: gid, botActivo: true }).lean().catch(() => null);
                            if (cfgD?.esPrincipal) { targetGid = gid; break; }
                            if (!targetGid && cfgD) targetGid = gid;
                        }
                        if (!targetGid) return;
                        const ANUNCIOS_DEPLOY = [
                            `*BEYONDER v${DEPLOY_VERSION} ONLINE*\n\nMe actualizaron. Volvi mas fuerte y con mejor memoria.\nNuevo parche cargado. A ver quien se porta mal primero.`,
                            `UPDATE COMPLETE — Beyonder recargado\n\nMe bajaron del cielo y me subieron de vuelta.\nCarlos Yoxel trabajo duro. Denle las gracias, no a mi.`,
                            `BEYONDER DE VUELTA\nVersion ${DEPLOY_VERSION} cargada.\n\nMemoria fresca, codigo limpio. Sigo siendo yo. Cuidense.`,
                        ];
                        await sock.sendMessage(targetGid, {
                            text: ANUNCIOS_DEPLOY[Math.floor(Math.random() * ANUNCIOS_DEPLOY.length)]
                        });
                    } catch(_) {}
                }, 5000); // esperar 5s para que el socket esté estable
            }
        }
        if (connection === 'close') {
            const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
            console.warn(`· desconectado [${code}] | intento ${_retryCount + 1}/${MAX_RETRIES}`);

            const needsReauth = code === 401 || code === 403 || code === DisconnectReason.loggedOut;

            if (needsReauth) {
                console.log('· sesión inválida — limpiando auth y reiniciando...');
                try { await Auth.deleteMany({}); console.log('· auth limpiado'); } catch(_) {}
                _pairingRequested = false;
                _retryCount = 0;
                setTimeout(startBot, 3000);
                return;
            }

            _retryCount++;
            if (_retryCount > MAX_RETRIES) {
                const waitMin = 5;
                console.error(`· ⛔ Demasiados reintentos (${MAX_RETRIES}). Esperando ${waitMin} minutos antes de reintentar...`);
                _retryCount = 0;
                setTimeout(startBot, waitMin * 60 * 1000);
                return;
            }

            // Backoff exponencial: 5s, 10s, 20s, 40s... máx 60s
            const delay = Math.min(5000 * Math.pow(2, _retryCount - 1), 60000);
            console.log(`· reintentando en ${Math.round(delay/1000)}s...`);
            setTimeout(startBot, delay);
        }
    });

    const sendText = async (jid, text, mentions = []) => {
        try { await sock.sendMessage(jid, { text, mentions }); }
        catch (e) { console.error('· sendText error:', e.message); }
    };
    const react = async (msg, emoji) => {
        try { await sock.sendMessage(msg.key.remoteJid, { react: { text: emoji, key: msg.key } }); } catch (_) {}
    };
    const del = async (msg) => {
        try { await sock.sendMessage(msg.key.remoteJid, { delete: msg.key }); } catch (_) {}
    };

    // Envía lista en bloques de 5 etiquetas (límite de WhatsApp por mensaje)
    const sendChunked = async (jid, header, items, getLine, getMention) => {
        const CHUNK = 5;
        for (let i = 0; i < items.length; i += CHUNK) {
            const chunk = items.slice(i, i + CHUNK);
            const txt = (i === 0 ? header : '') + chunk.map(getLine).join('');
            const mentions = chunk.map(getMention).filter(Boolean);
            await sendText(jid, txt, mentions);
        }
    };

    // limpiarDupDisponibles: elimina entradas en Disponible que sean el mismo
    // personaje que nombreNuevo según la IA (typos / alias)
    const limpiarDupDisponibles = async (commId, nombreNuevo) => {
        try {
            const disps = await Disponible.find({ groupId: commId });
            const otros = disps.map(d => d.personaje)
                .filter(n => n && normalizarNombre(n) !== normalizarNombre(nombreNuevo));
            if (!otros.length) return;
            const dup = await detectarDuplicadoDisponible(nombreNuevo, otros);
            if (dup) {
                await Disponible.deleteMany({ groupId: commId, personaje: dup });
                console.log(`· IA disponibles: "${dup}" ≈ "${nombreNuevo}" → eliminado`);
            }
        } catch(e) { console.error('· limpiarDupDisponibles:', e.message); }
    };

    // Cache de metadatos por grupo (se invalida en cambios de participantes)
    const metaCache = new Map();
    sock.ev.on('group-participants.update', ({ id }) => metaCache.delete(id));

    // Obtiene el groupId del set principal (o secundario) de la comunidad del grupo dado
    async function getPrincipalGid(gid) {
        try {
            const todos = await sock.groupFetchAllParticipating();
            const linked = todos[gid]?.linkedParent;
            if (!linked) return null;
            const subs = Object.values(todos).filter(g => g.linkedParent === linked);
            for (const g of subs) {
                const c = await Config.findOne({ groupId: g.id, esPrincipal: true });
                if (c) return g.id;
            }
            for (const g of subs) {
                const c = await Config.findOne({ groupId: g.id, esSecundario: true });
                if (c) return g.id;
            }
        } catch(_) {}
        return null;
    }

    // Alerta en set principal/secundario o en el propio grupo
    async function alertarEnPrincipal(communityGid, texto, mentions) {
        const alertGid = await getPrincipalGid(communityGid) || communityGid;
        await sendText(alertGid, texto, mentions || []);
    }

    // ─────────────────────────────────────────────
    // PROTECCIÓN NOMBRE/DESCRIPCIÓN DEL GRUPO
    // ─────────────────────────────────────────────
    sock.ev.on('groups.update', async updates => {
        for (const upd of updates) {
            metaCache.delete(upd.id);
            try {
                const cfgUpd = await Config.findOne({ groupId: upd.id });
                if (!cfgUpd) continue;

                if (upd.subject && cfgUpd.savedSubject && upd.subject !== cfgUpd.savedSubject) {
                    try {
                        await sock.groupUpdateSubject(upd.id, cfgUpd.savedSubject);
                        const alertGid = await getPrincipalGid(upd.id) || upd.id;
                        await sendText(alertGid, '⚠️ Intento de cambio de nombre del grupo revertido.');
                    } catch(_) {}
                }
                if (upd.desc !== undefined && cfgUpd.savedDesc !== null && upd.desc !== cfgUpd.savedDesc) {
                    try {
                        await sock.groupUpdateDescription(upd.id, cfgUpd.savedDesc || '');
                    } catch(_) {}
                }
            } catch(e) {}
        }
    });

    // Cache de metadatos "stale" — se usa como fallback en timeouts
    const metaStalCache = new Map(); // nunca expira, solo se actualiza

    const getMeta = async (gid, _retry = 0) => {
        // 1. Cache caliente (< 5 min)
        if (metaCache.has(gid)) return metaCache.get(gid);
        try {
            // Race entre groupMetadata y un timeout de 8s
            const meta = await Promise.race([
                sock.groupMetadata(gid),
                new Promise((_, rej) => setTimeout(() => rej(new Error('Timed Out')), 8000)),
            ]);
            metaCache.set(gid, meta);
            metaStalCache.set(gid, meta);               // guardar en stale
            setTimeout(() => metaCache.delete(gid), 5 * 60 * 1000);
            return meta;
        } catch (e) {
            // 2. Primer timeout → reintentar una vez después de 1.5s
            if (e.message === 'Timed Out' && _retry === 0) {
                await new Promise(r => setTimeout(r, 1500));
                return getMeta(gid, 1);
            }
            // 3. Segundo fallo → usar datos obsoletos si existen (grupo no cambia tan rápido)
            if (metaStalCache.has(gid)) {
                console.warn('· getMeta fallback stale cache para', gid);
                return metaStalCache.get(gid);
            }
            // 4. Sin datos — loguear solo errores no-triviales
            if (!e.message?.includes('Timed Out')) console.error('· getMeta error:', e.message);
            return null;
        }
    };
    const isAdm      = (meta, uid) => meta?.participants?.some(p => p.id === uid && (p.admin === 'admin' || p.admin === 'superadmin')) || false;
    const getAdmins  = (meta) => (meta?.participants || []).filter(p => p.admin === 'admin' || p.admin === 'superadmin').map(p => p.id);
    const getCreator = (meta) => meta?.participants?.find(p => p.admin === 'superadmin')?.id || null;

    // Devuelve el primer nombre de un personaje (antes del espacio o /)
    // Ej: "Marinette Dupain-Cheng / Ladybug" → "Marinette"
    function primerNombre(personaje) {
        if (!personaje) return personaje;
        return personaje.split(/[\s\/]/)[0].trim();
    }


    // Devuelve el nombre visible de un usuario (personaje completo, o null si no tiene)
    // Recibe el documento de User de MongoDB
    function nombreDisplay(u) {
        if (!u?.personaje) return null;
        return u.personaje;
    }
    // Devuelve todos los tokens (palabras + aliases tras /) de un personaje para matching
    // Ej: "Marinette Dupain-Cheng / Ladybug" → ["marinette","dupain-cheng","ladybug"]
    function tokensPersonaje(personaje) {
        if (!personaje) return [];
        const norm = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim();
        return personaje.split(/\//).flatMap(p => p.trim().split(/\s+/)).map(norm).filter(Boolean);
    }

    // ─────────────────────────────────────────────
    // MOTOR DE SCORING — resolución de identidad sin IA, sin colisiones
    // ─────────────────────────────────────────────
    function calcularScorePersonaje(personajeCompleto, busqueda) {
        if (!personajeCompleto || !busqueda) return 0;
        const normPC  = normalizarNombre(personajeCompleto);
        const normBus = normalizarNombre(busqueda);
        if (!normBus) return 0;

        // +100 exacta completa
        if (normPC === normBus) return 100;

        // +80 primer nombre (antes del primer espacio o /)
        const primerFrag = personajeCompleto.split(/[\s\/]/)[0].trim();
        if (normalizarNombre(primerFrag) === normBus) return 80;

        // +60 alias exacto (split por /)
        const aliases = personajeCompleto.split('/').map(s => s.trim()).filter(Boolean);
        for (const alias of aliases) {
            if (normalizarNombre(alias) === normBus) return 60;
        }

        // +40 token completo (palabra exacta en el nombre)
        const tokensNombre = normPC.replace(/[\/]/g, ' ').split(/\s+/).filter(Boolean);
        if (tokensNombre.includes(normBus)) return 40;

        // +10 parcial clásica
        if (normPC.includes(normBus)) return 10;

        return 0;
    }

    // Resuelve target con scoring heurístico — ganador claro o desempate por lastSeen
    async function resolveTarget(communityId, rawText, existingTarget) {
        if (existingTarget) return existingTarget;
        const parts = rawText.split(' ');
        if (parts.length < 2) return null;
        const namePart = parts.slice(1).join(' ').replace(/@\d+/g, '').trim();
        if (!namePart) return null;

        const users = await User.find({ groupId: communityId, personaje: { $ne: null } });
        if (!users.length) return null;

        const scored = users
            .map(u => ({ user: u, score: calcularScorePersonaje(u.personaje || '', namePart) }))
            .filter(e => e.score > 0)
            .sort((a, b) => b.score - a.score);

        if (!scored.length) return null;
        const topScore = scored[0].score;
        const topGroup = scored.filter(e => e.score === topScore);

        // Ganador único
        if (topGroup.length === 1) return topGroup[0].user.userId;

        // Empate en score bajo (parcial) → ambigüedad
        if (topScore <= 10) return { ambiguous: true, opciones: topGroup.map(e => e.user) };

        // Empate en score alto → desempate por lastSeen
        topGroup.sort((a, b) => {
            const tA = a.user.lastSeen ? new Date(a.user.lastSeen).getTime() : 0;
            const tB = b.user.lastSeen ? new Date(b.user.lastSeen).getTime() : 0;
            return tB - tA;
        });
        return topGroup[0].user.userId;
    }

    // Resuelve múltiples targets desde menciones + nombres en el texto
    // Retorna { targets: [...jids], ambiguous: { nombre, opciones } | null }
    async function resolveMultipleTargets(communityId, rawText, mentions) {
        const results = [];
        for (const m of mentions) if (m && !results.includes(m)) results.push(m);
        const cmd = rawText.split(' ')[0];
        const rest = rawText.slice(cmd.length).replace(/@\d+/g,'').trim();
        if (rest) {
            const partes = rest.split(/,|\sy\s/).map(s => s.trim()).filter(Boolean);
            for (const parte of partes) {
                const fakeText = `x ${parte}`;
                const r = await resolveTarget(communityId, fakeText, null);
                if (!r) continue;
                if (typeof r === 'string') {
                    if (!results.includes(r)) results.push(r);
                } else if (r.ambiguous) {
                    return { targets: [], ambiguous: { nombre: parte, opciones: r.opciones } };
                }
            }
        }
        return { targets: results, ambiguous: null };
    }

    async function resolve(communityId, rawText, existingTarget, replyFn) {
        const r = await resolveTarget(communityId, rawText, existingTarget);
        if (!r) return null;
        if (typeof r === 'string') return r;
        if (r.ambiguous) {
            const lista = r.opciones.map((u,i) => `${i+1}. *${u.personaje}*`).join('\n');
            await replyFn(`⚠️ *Varios personajes coinciden con "${rawText.split(' ').slice(1).join(' ')}":*\n${lista}\n\n_Sé más específico._`);
        }
        return null;
    }

    // Busca el JID del bot en la lista de participantes — 100% confiable
    function getBotJid(meta) {
        // Usar BOT_NUM global si está disponible, sino calcular
        const num = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
        if (!num) return '';
        const found = meta?.participants?.find(p => p.id.split('@')[0] === num);
        return found ? found.id : num + '@s.whatsapp.net';
    }

    // ── Sancionar: cooldown 20s, 1 adv hasta MAX_ADV ──
    async function sancionar(groupId, authorId, motivo, msg, meta, extraKeys = [], cid = null, deleteHistory = false) {
        del(msg).catch(_=>{});
        // Owners son inmunes
        if (await isOwner(authorId)) return;
        const communityId = cid || groupId;
        const scKey = `${communityId}_${authorId}`;
        const lastSancion = sancionCooldown.get(scKey) || 0;
        if (Date.now() - lastSancion < 20000) return;
        sancionCooldown.set(scKey, Date.now());
        // Solo borrar historial si es flood (deleteHistory=true)
        if (deleteHistory) {
            const allKeys = spamMsgs.get(authorId) || [];
            for (const k of [...allKeys, ...extraKeys]) {
                try { await sock.sendMessage(groupId, { delete: k }); } catch (_) {}
            }
        }
        spamMsgs.set(authorId, []);
        // No incrementar si ya está en el máximo
        const existing = await User.findOne({ groupId: communityId, userId: authorId });
        if ((existing?.advs || 0) >= MAX_ADV) {
            const tLabel = existing.personaje ? `*${existing.personaje}*` : `@${numFromJid(authorId)}`;
            const admins = getAdmins(meta).filter(a => a !== authorId);
            await sendText(groupId,
                `╭━━━━━━━━━━━━━━━━━━━━━━━ \n` +
                `🚨 ALERTA STAFF ·  ${tLabel} — @${numFromJid(authorId)}\n\n 📄: _${motivo}_\n\n ` +
                admins.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' ') +
                `\n╰━━━━━━━━━━━━━━━━━━━━━━━`,
                [authorId, ...admins.slice(0,3)]);
            return;
        }
        const u = await User.findOneAndUpdate(
            { groupId: communityId, userId: authorId },
            { $inc: { advs: 1 }, $push: {
                warnLog:  { motivo, fecha: new Date(), by: 'bot' },
                staffLog: { accion: `Auto: ${motivo}`, fecha: new Date(), by: 'bot' }
            }},
            { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
        );
        await logAction(groupId, `Auto: ${motivo}`, 'bot', authorId);
        await notificarAdv(groupId, u, motivo, 'bot', meta, authorId);

        // ── Marcar como "palomo" global al llegar a MAX_ADV ──
        if ((u?.advs || 0) >= MAX_ADV) {
            const esFlood = motivo.includes('flood');
            updateGlobalMemory(authorId, communityId, {
                vinculoGlobal: 'palomo',
                '$addToSet': {
                    gruposProblemáticos: communityId,
                    tags: esFlood ? 'spammer' : 'sancionado',
                },
            }).catch(()=>{});
        }
    }

    // ── Notificación de adv unificada ──
    async function notificarAdv(groupId, u, motivo, by, meta, targetId) {
        const tLabel = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
        const admins = getAdmins(meta).filter(a => a !== u.userId);
        if (u.advs >= MAX_ADV) {
            const warnResumen = (u.warnLog||[]).slice(-MAX_ADV).map((w,i)=>`   ${i+1}. _${w.motivo}_`).join('\n');
            const cfg2 = await Config.findOne({ groupId });

            // Calcular amIAdmin localmente — usa isBotAdmin con soporte LID
            const botAdminCheck = isBotAdmin(meta, sock);

            const esGore = motivo.toLowerCase().includes('gore');
            const esNsfw = !esGore && (motivo.toLowerCase().includes('explícito') || motivo.toLowerCase().includes('nsfw'));
            const esAdv  = !esGore && !esNsfw;
            const debeAutoban = botAdminCheck && (
                (esGore && cfg2?.autobanGore) ||
                (esNsfw && cfg2?.autobanNsfw) ||
                (esAdv  && cfg2?.autobanAdv)
            );
            if (debeAutoban) {
                // Autoban: EXPULSAR PRIMERO (seguridad inmediata), registrar en DB después
                const communityId2 = meta?.linkedParent || groupId;
                let grupos = [groupId];
                try {
                    const todos = await sock.groupFetchAllParticipating();
                    if (meta?.linkedParent) {
                        grupos = [groupId, ...Object.values(todos)
                            .filter(g => g.linkedParent === meta.linkedParent && g.id !== groupId)
                            .map(g => g.id)];
                    }
                } catch(_) {}
                // ── PASO 1: expulsión paralela inmediata ──
                let n = 0;
                await Promise.all(grupos.map(gid =>
                    sock.groupParticipantsUpdate(gid, [u.userId], 'remove')
                        .then(() => n++).catch(() => {})
                ));
                // ── PASO 2: registro en DB (milisegundos después) ──
                await User.findOneAndUpdate({ groupId: communityId2, userId: u.userId },
                    { banned: true, inGroup: false, $push: { staffLog: { accion: `Autoban: ${motivo}`, fecha: new Date(), by: 'bot' } } });
                await logAction(groupId, `Autoban: ${motivo}`, 'bot', u.userId);
                await sendText(groupId,
                    `╭━━━━━━━━━━━━━━━━━━━━━━━ \n` +
                    `🔨 *AUTOBAN ON* ·  ${tLabel} — @${numFromJid(u.userId)}\n\nfue expulsado de *${n} grupo(s)* al llegar a *${MAX_ADV}/${MAX_ADV}*\n\n   *Historial:*\n${warnResumen}\n\n ` +
                    admins.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' ') +
                    `\n╰━━━━━━━━━━━━━━━━━━━━━━━`,
                    [u.userId, ...admins.slice(0,3)]);
            } else {
                // Autoban off — solo alertar al staff
                await sendText(groupId,
                    `╭━━━━━━━━━━━━━━━━━━━━━━━ \n` +
                    `🚨 ALERTA STAFF ·  ${tLabel} — @${numFromJid(u.userId)}\n\n\n` +
                    ` 📄: _${motivo}_\n\n   *Historial:*\n${warnResumen}\n\n` +
                    `  🔔 ·  Se requiere acción manual.\n ` +
                    admins.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' ') +
                    `\n╰━━━━━━━━━━━━━━━━━━━━━━━`,
                    [u.userId, ...admins.slice(0,3)]);
            }
        } else {
            await sendText(groupId, `🔔 ${tLabel} — ha recibido una advertencia. *${u.advs}/${MAX_ADV}*`, [u.userId]);
        }
    }

    // ─────────────────────────────────────────────
    // EVENTOS DE GRUPO — ban permanente al reentrar
    // ─────────────────────────────────────────────
    const expulsionTracker = new Map(); // userId → [timestamps] — para auto-degradar

    sock.ev.on('group-participants.update', async ({ id: groupId, participants, action, author }) => {
        try {
            const meta        = await getMeta(groupId);
            const communityId = meta?.linkedParent || groupId;

            // ── Auto-degradar si un no-owner (no bot) expulsa 3+ personas en <60s ──
            if (action === 'remove' && author && numFromJid(author) !== numFromJid(getBotJid(meta))) {
                const isAuthorOwner = await isOwner(author);
                if (!isAuthorOwner) {
                    const now = Date.now();
                    const times = (expulsionTracker.get(author) || []).filter(t => now - t < 60000);
                    times.push(...participants.map(() => now));
                    expulsionTracker.set(author, times);
                    if (times.length >= 3) {
                        expulsionTracker.delete(author);
                        try {
                            await sock.groupParticipantsUpdate(groupId, [author], 'demote');
                            const uDemote = await User.findOne({ groupId: communityId, userId: author });
                            const dLabel = uDemote?.personaje ? `*${uDemote.personaje}*` : `@${numFromJid(author)}`;
                            await sendText(groupId, `╭━━━━━━━━━━━━━━━━━━━━━━━\n🚨 ALERTA STAFF · ${dLabel} — @${numFromJid(author)}\n      expulsó ${times.length} miembros en menos de 60s\n\n  *degradado automáticamente.*\n╰━━━━━━━━━━━━━━━━━━━━━━━`, [author]);
                            await logAction(groupId, `Auto-degradado por expulsiones masivas`, 'bot', author);
                        } catch(e) { console.error('· auto-demote error:', e.message); }
                    }
                }
            }

            // ── Bot expulsado del grupo ──
            const botJidGpe = getBotJid(meta);
            const botNumGpe = numFromJid(botJidGpe);
            if (action === 'remove' && participants.some(p => numFromJid(p) === botNumGpe)) {
                try {
                    const alertGid = await getPrincipalGid(groupId);
                    const adminsToAlert = alertGid ? getAdmins(await getMeta(alertGid)) : [];
                    const txt = alertGid
                        ? '🔔 Beyonder fue expulsado de un grupo. ' + adminsToAlert.slice(0,5).map(a => '@' + numFromJid(a)).join(' ')
                        : '🔔 Beyonder fue expulsado de la comunidad. AUXILIO.';
                    await sendText(alertGid || communityId, txt, adminsToAlert.slice(0,5));
                } catch(_) {}
            }

            // ── Acciones confi: promote/demote sin usar comando del bot ──
            // IMPORTANTE: comparar JIDs normalizados para evitar falsos positivos
            // con formato multi-device (número:dispositivo@s.whatsapp.net)
            const botNumNorm = numFromJid(botJidGpe);
            const authorNum  = numFromJid(author || '');
            const esBotAuthor = !!author && authorNum === botNumNorm;

            if ((action === 'promote' || action === 'demote') && author && !esBotAuthor) {
                const isAuthorOwner = await isOwner(author);
                if (!isAuthorOwner) {
                    const uAuthor = await User.findOne({ groupId: communityId, userId: author });
                    const authorConfi = !!uAuthor?.confi;
                    const alertGid = await getPrincipalGid(groupId) || groupId;
                    const adminsAll = getAdmins(meta).filter(a => numFromJid(a) !== authorNum);

                    for (const userId of participants) {
                        // Si fue acción iniciada por el bot vía comando, ignorar
                        const botActionKey = `${groupId}:${userId}:${action}`;
                        if (pendingBotActions.has(botActionKey)) {
                            pendingBotActions.delete(botActionKey);
                            continue;
                        }

                        if (action === 'promote') {
                            const uTarget = await User.findOne({ groupId: communityId, userId });
                            if (!uTarget?.confi) {
                                // Promovió a alguien sin confi → degradar a ambos
                                try { markBotAction(groupId, userId, 'demote'); await sock.groupParticipantsUpdate(groupId, [userId], 'demote'); } catch(_) {}
                                try { markBotAction(groupId, author, 'demote'); await sock.groupParticipantsUpdate(groupId, [author], 'demote'); } catch(_) {}
                                await User.findOneAndUpdate({ groupId: communityId, userId: author },
                                    { desconfi: true, desconfiSince: new Date() }, { upsert: true });
                                const aLabel = uAuthor?.personaje ? '*' + uAuthor.personaje + '*' : '@' + numFromJid(author);
                                const tLabel = uTarget?.personaje ? '*' + uTarget.personaje + '*' : '@' + numFromJid(userId);
                                const alertMsg = '╭━━━━━━━━━━━━━━━━━━━━━━━ \n' +
                                    '🚨 ALERTA STAFF ·  ' + aLabel + ' — @' + numFromJid(author) + '\n      promovió a ' + tLabel + ' (sin confi)\n\n  *ambos degradados y sancionados*\n ' +
                                    adminsAll.slice(0,4).map(a => '@' + numFromJid(a)).join(' ') + '\n╰━━━━━━━━━━━━━━━━━━━━━━━';
                                await sendText(alertGid, alertMsg, [author, userId, ...adminsAll.slice(0,4)]);
                                await logAction(groupId, 'Auto-degradado: promovió sin confi', 'bot', author);
                            }
                            // Target con confi → promoción válida, no hacer nada
                        } else if (action === 'demote') {
                            const uTarget = await User.findOne({ groupId: communityId, userId });
                            if (uTarget?.confi) {
                                // Degradó a alguien con confi → revertir + sancionar
                                try { markBotAction(groupId, author, 'demote'); await sock.groupParticipantsUpdate(groupId, [author], 'demote'); } catch(_) {}
                                try { markBotAction(groupId, userId, 'promote'); await sock.groupParticipantsUpdate(groupId, [userId], 'promote'); } catch(_) {}
                                await User.findOneAndUpdate({ groupId: communityId, userId: author },
                                    { desconfi: true, desconfiSince: new Date() }, { upsert: true });
                                const aLabel = uAuthor?.personaje ? '*' + uAuthor.personaje + '*' : '@' + numFromJid(author);
                                const tLabel = uTarget?.personaje ? '*' + uTarget.personaje + '*' : '@' + numFromJid(userId);
                                const alertMsg = '╭━━━━━━━━━━━━━━━━━━━━━━━ \n' +
                                    '🚨 ALERTA STAFF ·  ' + aLabel + ' — @' + numFromJid(author) + '\n      degradó a ' + tLabel + ' [confi]\n\n  *revertido y sancionado.*\n ' +
                                    adminsAll.slice(0,4).map(a => '@' + numFromJid(a)).join(' ') + '\n╰━━━━━━━━━━━━━━━━━━━━━━━';
                                await sendText(alertGid, alertMsg, [author, userId, ...adminsAll.slice(0,4)]);
                                await logAction(groupId, 'Auto-degradado: degradó a confi', 'bot', author);
                            } else if (!authorConfi) {
                                // Admin sin confi degradó a alguien sin confi → sancionar autor
                                try { markBotAction(groupId, author, 'demote'); await sock.groupParticipantsUpdate(groupId, [author], 'demote'); } catch(_) {}
                                await User.findOneAndUpdate({ groupId: communityId, userId: author },
                                    { desconfi: true, desconfiSince: new Date() }, { upsert: true });
                            }
                            // Autor confi + target sin confi → permitido
                        }
                    }
                }
            }

            // ── Expulsión manual por admin sin confi (sin usar comando del bot) ──
            // Regla del "Autocidio": en Baileys, salida voluntaria = author vacío o igual al participante
            const esSalidaVoluntaria = action === 'remove' && (
                !author ||
                participants.includes(author) ||
                (participants.length === 1 && authorNum === numFromJid(participants[0]))
            );

            // esBotRemove: el bot marcó esta expulsión con markBotAction
            const esBotRemove = action === 'remove' &&
                participants.some(p => pendingBotActions.has(`${groupId}:${p}:remove`));

            if (action === 'remove' && author && !esBotAuthor && !esSalidaVoluntaria && !esBotRemove) {
                const isAuthorOwner = await isOwner(author);
                if (!isAuthorOwner) {
                    const uAuthor = await User.findOne({ groupId: communityId, userId: author });
                    if (!uAuthor?.confi) {
                        const alertGid = await getPrincipalGid(groupId) || groupId;
                        const adminsAll = getAdmins(meta).filter(a => numFromJid(a) !== authorNum);
                        let grupos = [groupId];
                        if (meta?.linkedParent) {
                            try {
                                const todos = await sock.groupFetchAllParticipating();
                                grupos = Object.values(todos).filter(g => g.linkedParent === meta.linkedParent).map(g => g.id);
                            } catch(_) {}
                        }
                        for (const gid of grupos) {
                            markBotAction(gid, author, 'demote'); try { await sock.groupParticipantsUpdate(gid, [author], 'demote'); } catch(_) {}
                        }
                        await User.findOneAndUpdate({ groupId: communityId, userId: author },
                            { desconfi: true, desconfiSince: new Date() }, { upsert: true });
                        const aLabel = uAuthor?.personaje ? '*' + uAuthor.personaje + '*' : '@' + numFromJid(author);
                        await sendText(alertGid,
                            '╭━━━━━━━━━━━━━━━━━━━━━━━ \n' +
                            '🚨 ALERTA STAFF ·  ' + aLabel + ' — @' + numFromJid(author) + '\n      expulsó a un miembro sin autorización\n\n  *degradado de todo y sancionado.*\n ' +
                            adminsAll.slice(0,5).map(a => '@' + numFromJid(a)).join(' ') + '\n╰━━━━━━━━━━━━━━━━━━━━━━━',
                            [author, ...adminsAll.slice(0,5)]);
                        await logAction(groupId, 'Auto-degradado: expulsión sin confi', 'bot', author);
                    }
                }
            }

            for (const userId of participants) {
                // ── Alguien entra ──
                if (action === 'add') {
                    // Upsert inmediato con inGroup:true — garantiza lista real para !inactivos
                    await User.findOneAndUpdate(
                        { groupId: communityId, userId },
                        {
                            $set: { inGroup: true },
                            $setOnInsert: { joinDate: new Date(), desconfiSince: new Date(), desconfi: true, personaje: null }
                        },
                        { upsert: true, setDefaultsOnInsert: true }
                    ).catch(() => {});
                    const u = await User.findOne({ groupId: communityId, userId });
                    if (u?.banned) {
                        if (isAdm(meta, getBotJid(meta))) {
                            try { await sock.groupParticipantsUpdate(groupId, [userId], 'remove'); } catch(e) {}
                            await sendText(groupId, '⤷ ゛🚫  ˎˊ˗\n♯ ·  · *Intento fallido.*\nUn usuario baneado intentó entrar y fue\nexpulsado automáticamente. Aquí no entra.', []);
                        }
                        continue;
                    }
                    // Bienvenida desactivada
                    continue;
                }

                // ── Alguien sale o es expulsado ──
                if (action === 'remove') {
                    // Si fue el bot quien expulsó (ej: !ban), no emitir mensaje
                    const botRemoveKey = `${groupId}:${userId}:remove`;
                    if (pendingBotActions.has(botRemoveKey)) {
                        pendingBotActions.delete(botRemoveKey);
                        // Igual procesar datos (liberar personaje, etc.) pero sin mensaje
                        const u = await User.findOne({ groupId: communityId, userId });
                        let sigueEnComunidad = false;
                        if (meta?.linkedParent) {
                            try {
                                const todosGrupos = await sock.groupFetchAllParticipating();
                                const gruposCom = Object.values(todosGrupos).filter(g => g.linkedParent === meta.linkedParent && g.id !== groupId);
                                for (const g of gruposCom) {
                                    if (g.participants?.some(p => p.id === userId)) { sigueEnComunidad = true; break; }
                                }
                            } catch(_) {}
                        }
                        if (!sigueEnComunidad && u) {
                            if (u.personaje) await Disponible.create({ groupId: communityId, personaje: u.personaje }).catch(() => {});
                            const isOwnerU = await isOwner(userId);
                            const meses = (isOwnerU || u.confi) ? 2 : 1;
                            const dataExpira = new Date(Date.now() + meses * 30 * 24 * 60 * 60 * 1000);
                            await User.findOneAndUpdate({ groupId: communityId, userId },
                                { $set: { personaje: null, exitDate: new Date(), dataExpira } });
                            await Pareja.deleteMany({ groupId: communityId, $or: [{ user1: userId }, { user2: userId }] }).catch(() => {});
                            await SolicitudPareja.deleteMany({ groupId: communityId, $or: [{ solicitante: userId }, { solicitado: userId }] }).catch(() => {});
                            await SolicitudIntercambio.deleteMany({ groupId: communityId, $or: [{ solicitante: userId }, { solicitado: userId }] }).catch(() => {});
                        }
                        continue;
                    }

                    const u = await User.findOne({ groupId: communityId, userId });

                    // Verificar si sigue en algún grupo de la comunidad
                    let sigueEnComunidad = false;
                    if (meta?.linkedParent) {
                        try {
                            const todosGrupos = await sock.groupFetchAllParticipating();
                            const gruposCom = Object.values(todosGrupos).filter(g => g.linkedParent === meta.linkedParent && g.id !== groupId);
                            for (const g of gruposCom) {
                                if (g.participants?.some(p => p.id === userId)) { sigueEnComunidad = true; break; }
                            }
                        } catch(_) {}
                    }

                    // Liberar personaje si salió de toda la comunidad; guardar datos con expiración
                    if (!sigueEnComunidad) {
                        if (u?.personaje) {
                            await Disponible.create({ groupId: communityId, personaje: u.personaje }).catch(() => {});
                        }
                        if (u) {
                            const isOwnerU = await isOwner(userId);
                            const meses = (isOwnerU || u.confi) ? 2 : 1;
                            const dataExpira = new Date(Date.now() + meses * 30 * 24 * 60 * 60 * 1000);
                            await User.findOneAndUpdate({ groupId: communityId, userId },
                                { $set: { personaje: null, exitDate: new Date(), dataExpira, inGroup: false } });
                            await Pareja.deleteMany({ groupId: communityId, $or: [{ user1: userId }, { user2: userId }] }).catch(() => {});
                            await SolicitudPareja.deleteMany({ groupId: communityId, $or: [{ solicitante: userId }, { solicitado: userId }] }).catch(() => {});
                            await SolicitudIntercambio.deleteMany({ groupId: communityId, $or: [{ solicitante: userId }, { solicitado: userId }] }).catch(() => {});
                        }
                    }

                    // ── Salida voluntaria: log silencioso en DB, sin alertas de staff ──
                    // (esSalidaVoluntaria fue calculado antes del bucle de participantes)
                    const esVoluntaria = !author || participants.includes(author) ||
                        (participants.length === 1 && numFromJid(author) === numFromJid(userId));

                    if (esVoluntaria) {
                        console.log(`· salida voluntaria silenciosa: ${numFromJid(userId)}`);
                        if (u?.personaje && !sigueEnComunidad) {
                            await sendText(groupId,
                                `  ⤷ ゛🍃  ˎˊ˗\n  ♯ ·  · *${u.personaje}* se ha ido.\n  _Que le vaya bien._`, []);
                        }
                        continue; // NO alertas de staff
                    }

                    // ── Expulsión admin: armar mensaje de baja ──
                    let lines = [];
                    if (u?.personaje && !sigueEnComunidad) {
                        lines.push('🎭 *Personaje liberado:* _"' + u.personaje + '"_ — ya disponible @' + numFromJid(userId));
                    } else if (u?.personaje && sigueEnComunidad) {
                        lines.push('🎭 *Personaje:* _"' + u.personaje + '"_ — sigue en otro grupo, no se libera');
                    }
                    if (u?.advs > 0) {
                        lines.push('⚠️ *Advertencias acumuladas:* ' + u.advs + '/3');
                    }
                    if (lines.length > 0) await sendText(groupId, lines.join('\n'), [userId]);
                }
            }
        } catch (e) { console.error('· group-participants:', e.message); }
    });

    // ── Limpieza de datos expirados — corre cada 6 horas ──
    setInterval(async () => {
        try {
            await User.deleteMany({ dataExpira: { $lt: new Date() } });
        } catch(_) {}
    }, 6 * 60 * 60 * 1000);

    // ── Bot expulsado de una comunidad — avisar a owners por privado ──
    sock.ev.on('groups.leave', async (groupIds) => {
        for (const gid of groupIds) {
            try {
                const meta = await getMeta(gid).catch(() => null);
                if (meta?.linkedParent) {
                    // Era subgrupo de comunidad — ya se maneja en group-participants
                } else {
                    // Era una comunidad o grupo raíz
                    const ownerDocs = await Owner.find({});
                    for (const o of ownerDocs) {
                        const ownerJid = o.userId + '@s.whatsapp.net';
                        await sock.sendMessage(ownerJid, { text: '⚠️ *El bot fue expulsado de una comunidad/grupo.* (' + gid + ')' }).catch(() => {});
                    }
                }
            } catch(_) {}
        }
    });

    // ─────────────────────────────────────────────
    // MENSAJES
    // ─────────────────────────────────────────────
    // Lock de procesamiento — evita que el mismo mensaje se procese dos veces
    // (previene el flood de 20 mensajes al configurar !set secundario, etc.)
    const processingMsgIds = new Set();

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
        if (type !== 'notify') return;

        for (const message of messages) {
            // ── Deduplicación por ID de mensaje ──
            const msgId = message.key?.id;
            if (msgId) {
                if (processingMsgIds.has(msgId)) continue; // ya en proceso — ignorar
                processingMsgIds.add(msgId);
                setTimeout(() => processingMsgIds.delete(msgId), 15000); // limpiar tras 15s
            }
            try {
                const groupId = message.key.remoteJid;

                // ══════════════════════════════════════════════════════
                // BARRERA #1 — ANTI-MAMÁ (FILTRO DE PRIVADOS)
                // Si es un DM y el remitente NO es el owner → ignorar
                // completamente. Ni comandos, ni IA, ni nada.
                // ══════════════════════════════════════════════════════
                if (!groupId?.endsWith('@g.us')) {
                    // No es grupo → es DM (o newsletter, etc.)
                    if (!groupId?.endsWith('@s.whatsapp.net')) continue; // ignorar newsletters
                    if (OWNER_JID && groupId !== OWNER_JID) continue;   // Anti-Mamá: corte total
                }

                const rawTextAny = message.message?.conversation
                    || message.message?.extendedTextMessage?.text
                    || message.message?.imageMessage?.caption
                    || message.message?.videoMessage?.caption
                    || message.message?.documentMessage?.caption
                    || message.message?.buttonsResponseMessage?.selectedDisplayText
                    || message.message?.listResponseMessage?.title
                    || message.message?.templateButtonReplyMessage?.selectedDisplayText
                    || '';
                const bodyAny = rawTextAny.toLowerCase().trim();

                // ── DM: pasó el filtro → es el owner hablando en privado ──
                if (!groupId?.endsWith('@g.us')) {
                    if (!rawTextAny.trim()) continue;
                    const dmUserId      = groupId;
                    const dmCommunityId = 'dm_' + numFromJid(groupId);

                    // Guardar cada mensaje en contexto
                    saveGroupMessage(dmCommunityId, dmUserId, 'yo', rawTextAny.trim()).catch(() => {});

                    // ── DEBOUNCE DM ──
                    const dmDebounceKey = `dm_${dmUserId}`;
                    const dmExisting    = _msgDebounce.get(dmDebounceKey);
                    const dmParts       = dmExisting ? dmExisting.parts : [];
                    dmParts.push(rawTextAny.trim());
                    if (dmExisting?.timer) clearTimeout(dmExisting.timer);

                    const dmTimer = setTimeout(async () => {
                        _msgDebounce.delete(dmDebounceKey);
                        const fullDM = dmParts.join('\n');
                        try {
                            const ctx      = await getContextoIA(dmCommunityId, dmUserId, null, 'default');
                            await simulateTyping(sock, groupId);
                            const replyText = await obtenerRespuestaIA(fullDM, ctx.nombreUsuario, ctx);
                            if (replyText) {
                                await humanDelay(sock, groupId);
                                await sock.sendMessage(groupId, { text: replyText });
                                updateResumenPersonalidad(dmCommunityId, dmUserId, fullDM, replyText).catch(() => {});
                            }
                        } catch (e) { console.error('· DM reply error:', e.message); }
                    }, DEBOUNCE_MS);

                    _msgDebounce.set(dmDebounceKey, { timer: dmTimer, parts: dmParts });
                    continue;
                }

                const authorId = message.key.participant || message.participant;
                if (!authorId) continue;
                if (BOT_NUM && numFromJid(authorId) === BOT_NUM) continue;

                const rawText = message.message?.conversation
                    || message.message?.extendedTextMessage?.text
                    || message.message?.imageMessage?.caption
                    || message.message?.videoMessage?.caption
                    || message.message?.documentMessage?.caption
                    || message.message?.buttonsResponseMessage?.selectedDisplayText
                    || message.message?.listResponseMessage?.title
                    || message.message?.templateButtonReplyMessage?.selectedDisplayText
                    || '';
                const body = rawText.toLowerCase().trim();

                const reply = async (text, mentions = [], _skipFatigue = false) => {
                    // Guard: socket cerrado durante reconexión → descartar silenciosamente
                    if (!sock?.user) return;
                    try {
                        let finalText = text;
                        // Añadir sarcasmo de fatiga al final (niveles 1-2 solamente)
                        if (!_skipFatigue && message._fatiga && message._fatiga.nivel >= 1 && message._fatiga.nivel <= 2) {
                            const fatigaComentario = getFatigeResponse(message._fatiga.nivel);
                            finalText = text + '\n\n_' + fatigaComentario + '_';
                            message._fatiga = null; // disparar solo una vez
                        }
                        await sock.sendMessage(groupId, { text: finalText, mentions }, { quoted: message });
                        _lastBotMsgTime.set(communityId, Date.now());
                    } catch (e) {
                        // Connection Closed / Stream Errored son esperables durante reconexión — suprimir
                        const dead = e.message?.includes('Connection Closed') ||
                                     e.message?.includes('Stream Errored') ||
                                     e.message?.includes('Connection Failure');
                        if (!dead) console.error('· reply error:', e.message);
                        if (dead) return; // no reintentar con socket muerto
                        try { await sock.sendMessage(groupId, { text, mentions }, { quoted: message }); }
                        catch(e2) {
                            const dead2 = e2.message?.includes('Connection Closed') || e2.message?.includes('Stream Errored');
                            if (!dead2) console.error('· reply retry error:', e2.message);
                            if (!dead2) {
                                try { await sock.sendMessage(groupId, { text, mentions }); }
                                catch(e3) { if (!e3.message?.includes('Connection')) console.error('· reply bare error:', e3.message); }
                            }
                        }
                    }
                };

                // ── 1. Meta — communityId depende de linkedParent ──
                const meta        = await getMeta(groupId);
                const botJid      = getBotJid(meta);
                const isAdmin     = isAdm(meta, authorId);
                const communityId = meta?.linkedParent || groupId;

                // ── 2. Fire-and-forget: contadores y logs (no bloquean nada) ──
                const msgTipo = message.message?.imageMessage ? 'imagen' : message.message?.videoMessage ? 'video'
                    : message.message?.stickerMessage ? 'sticker' : message.message?.audioMessage ? 'audio'
                    : message.message?.documentMessage ? 'doc' : 'texto';
                trackMsg(authorId, message.key);
                // Actividad diaria — 10 msgs = día activo (comunidad-wide)
                (async () => {
                    try {
                        const hoy = new Date().toISOString().slice(0,10);
                        const uAct = await User.findOne({ groupId: communityId, userId: authorId });
                        const mismodia = uAct?.dailyMsgDate === hoy;
                        const nuevoCount = mismodia ? (uAct.dailyMsgCount || 0) + 1 : 1;
                        const setFields = { lastSeen: new Date(), dailyMsgCount: nuevoCount, dailyMsgDate: hoy };
                        if (nuevoCount >= 10) setFields.lastActiveDay = new Date();
                        await User.findOneAndUpdate(
                            { groupId: communityId, userId: authorId },
                            { $inc: { msgCount: 1 }, $set: setFields, $setOnInsert: { joinDate: new Date() } },
                            { upsert: true, setDefaultsOnInsert: true }
                        );
                    } catch(_) {}
                })();
                MsgLog.create({ groupId, userId: authorId, tipo: msgTipo, contenido: (rawText||'').slice(0,100) }).catch(()=>{});

                // ── Cerebro: slang + sentimiento local + memoria global (fire-and-forget) ──
                if (rawText) {
                    registrarSlang(communityId, rawText).catch(()=>{});
                    const mencionaBot = rawText.toLowerCase().includes('beyond') || rawText.toLowerCase().includes('beyonder');
                    if (mencionaBot) {
                        if (esInsultoAlBot(rawText)) {
                            updateSentimiento(communityId, authorId, -1).catch(()=>{});
                            updateGlobalMemory(authorId, communityId, { '$inc': { sentimientoGlobal: -1 } }).catch(()=>{});
                        } else if (esTratoBueno(rawText)) {
                            updateSentimiento(communityId, authorId, +1).catch(()=>{});
                            updateGlobalMemory(authorId, communityId, { '$inc': { sentimientoGlobal: 1 } }).catch(()=>{});
                        }
                    }
                    // Actividad global — actualiza lastSeenGlobal + gruposActivos en cada mensaje
                    updateGlobalMemory(authorId, communityId, {}).catch(()=>{});
                }

                // ── 3. Paralelo: solo lo necesario para ejecutar el comando ──
                const [cfgRaw, userData, isOW] = await Promise.all([
                    Config.findOne({ groupId }),
                    User.findOne({ groupId: communityId, userId: authorId }),
                    isGroupOwner(meta, authorId),
                ]);

                const botNum = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];

                // ── amIAdmin — soporta JID normal y formato LID (comunidades) ──
                const amIAdmin = isBotAdmin(meta, sock);
                const creator  = getCreator(meta);
                const target   = getTargetFromMsg(message);

                let cfg;
                try {
                    cfg = cfgRaw || await Config.create({ groupId });
                } catch(e) {
                    console.error('· Config error:', e.message);
                    cfg = { botActivo:true, antilink:true, autoclose:true, antiporno:true, antiflood:true, lockperso:false, autobanAdv:false };
                }

                // Modo silencioso — owners siempre pueden usar comandos aunque el bot esté off
                if (cfg.botActivo === false && !isOW) continue;

                // ── Silenciado — borrar todo lo que mande ──
                if (userData?.silenciado && !isAdmin && !isOW) {
                    await del(message);
                    continue;
                }

                // ── Tipos de media — declarar AQUÍ para que antiflood y antiporno los vean ──
                const isImage    = !!message.message?.imageMessage;
                const isSticker  = !!message.message?.stickerMessage;
                const isVideo    = !!message.message?.videoMessage;
                const isDocument = !!message.message?.documentMessage &&
                    /\.(jpe?g|png|webp|gif|bmp)$/i.test(message.message.documentMessage.fileName || '');

                // Ver una vez — parchear flag INMEDIATAMENTE para preservar clave de descifrado
                const viewOnceMsg = message.message?.viewOnceMessage?.message
                    || message.message?.viewOnceMessageV2?.message
                    || message.message?.viewOnceMessageV2Extension?.message;
                const isViewOnceImage = !!viewOnceMsg?.imageMessage;
                const isViewOnceVideo = !!viewOnceMsg?.videoMessage;
                if (viewOnceMsg?.imageMessage) viewOnceMsg.imageMessage.viewOnce = false;
                if (viewOnceMsg?.videoMessage) viewOnceMsg.videoMessage.viewOnce = false;

                // Quoted
                const quotedMsg  = message.message?.extendedTextMessage?.contextInfo?.quotedMessage;
                const isQuotedImage   = !!quotedMsg?.imageMessage;
                const isQuotedVideo   = !!quotedMsg?.videoMessage;
                const isQuotedSticker = !!quotedMsg?.stickerMessage;
                const quotedParticipant = message.message?.extendedTextMessage?.contextInfo?.participant;

                // ── Anti-flood RAM (primera capa — sin MongoDB) ──
                if (cfg.antiflood && !isAdmin && !isOW && checkFloodControl(authorId)) {
                    await sancionar(groupId, authorId, 'flood de mensajes', message, meta, [], communityId, true);
                    try { await sock.groupSettingUpdate(groupId, 'announcement'); } catch(_) {}
                    await sendText(groupId, `🔒 Grupo cerrado por flood. Se reabrirá en 30 segundos.`);
                    setTimeout(async () => {
                        try { await sock.groupSettingUpdate(groupId, 'not_announcement');
                            await sendText(groupId, `🔓 Grupo reabierto.`); } catch(_) {}
                    }, 30000);
                    floodControl.delete(authorId);
                    stickerMap.set(authorId, []);
                    allMsgMap.set(authorId, []);
                    continue;
                }

                // ── Anti-flood TEMPRANO — antes del análisis antiporno (evita que el delay lo rompa) ──
                // Stickers: 10+ en < 5s | General: 15+ mensajes cualquier tipo en < 10s
                if (cfg.antiflood && !isAdmin && !isOW) {
                    const isStickerF  = isSticker && checkStickerFlood(authorId);
                    const isGeneralF  = checkGeneralFlood(authorId);
                    if (isStickerF || isGeneralF) {
                        const motivoFlood = isStickerF ? 'flood de stickers' : 'flood de mensajes';
                        await sancionar(groupId, authorId, motivoFlood, message, meta, [], communityId, true);
                        // Cerrar grupo 30s
                        try { await sock.groupSettingUpdate(groupId, 'announcement'); } catch(_) {}
                        await sendText(groupId, `🔒 Grupo cerrado por flood. Se reabrirá en 30 segundos.`);
                        setTimeout(async () => {
                            try {
                                await sock.groupSettingUpdate(groupId, 'not_announcement');
                                await sendText(groupId, `🔓 Grupo reabierto automáticamente.`);
                            } catch (_) {}
                        }, 30000);
                        // Resetear contadores para no re-triggear en mensajes siguientes
                        stickerMap.set(authorId, []);
                        allMsgMap.set(authorId, []);
                        continue;
                    }
                }

                // Función reutilizable para analizar y sancionar ─────────────────
                // ── Tier: nivel de confianza → ajusta sensibilidad de NudeNet ──
                // desconfi/nuevo=0.90 (más sensible) | normal=1.0 | confi=1.08 | owner=1.15
                const tierMultiplier = (() => {
                    if (isOW) return 1.15;
                    if (!userData) return 0.90;         // sin datos = desconfi
                    if (userData.confi) return 1.08;
                    if (userData.desconfi) return 0.90;
                    if (userData.desconfiSince) {
                        const dias = (Date.now() - new Date(userData.desconfiSince)) / 86400000;
                        if (dias < 7) return 0.90;      // nuevo (<7 días)
                    }
                    return 1.0;
                })();

                // Desconfi reciben análisis prioritario (sin skip)
                const esDesconfiSender = tierMultiplier <= 0.90;

                const analizarYSancionar = async (bufferOrFrames, targetAuthorId, msgToDelete, isFrameArray = false) => {
                    let bad = false, tipoContenido = '', tipoDetalle = '';
                    const checkBuf = async (buf) => {
                        if (bad) return;
                        try {
                            const r = await queryNudeNet(buf);
                            if (!r) { console.log('· NudeNet sin respuesta — omitiendo frame'); return; }
                            const { tipo } = analizarResultadoNudeNet(r, false, tierMultiplier);
                            if (tipo) {
                                bad = true;
                                tipoContenido = tipo === 'gore' ? 'gore' : 'contenido explícito';
                            }
                        } catch(e) { console.error('· checkBuf error:', e.message); }
                    };
                    if (isFrameArray) {
                        await Promise.all(bufferOrFrames.map(f => checkBuf(f)));
                    } else await checkBuf(bufferOrFrames);

                    if (!bad) return false;

                    // Borrar INMEDIATAMENTE — no esperar la sanción
                    del(msgToDelete).catch(_=>{}); // fire and forget

                    // Sancionar al autor real del contenido (en paralelo al borrado)
                    const existing2 = await User.findOne({ groupId: communityId, userId: targetAuthorId });
                    const tLabel = existing2?.personaje ? `*${existing2.personaje}*` : `@${numFromJid(targetAuthorId)}`;
                    const adminsAlert = getAdmins(meta).filter(a => a !== targetAuthorId);

                    const cfgLocal = await Config.findOne({ groupId });
                    const esNsfwLocal = !tipoContenido.includes('gore');
                    // Si autobanNsfw está OFF y es nsfw reincidente → incrementar contador interno
                    if (esNsfwLocal && !cfgLocal?.autobanNsfw && (existing2?.advs || 0) >= MAX_ADV) {
                        const uNsfw = await User.findOneAndUpdate(
                            { groupId: communityId, userId: targetAuthorId },
                            { $inc: { nsfwCount: 1 }, $push: { staffLog: { accion: `NSFW reincidente #${(existing2.nsfwCount||0)+1}`, fecha: new Date(), by: 'bot' } } },
                            { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
                        );
                        if (uNsfw.nsfwCount >= 3) {
                            // Banear de todos los grupos
                            let grupos = [groupId];
                            if (meta?.linkedParent) {
                                try { const todos = await sock.groupFetchAllParticipating(); grupos = [groupId, ...Object.values(todos).filter(g=>g.linkedParent===meta.linkedParent&&g.id!==groupId).map(g=>g.id)]; } catch(_) {}
                            }
                            let nG = 0;
                            for (const gid of grupos) { try { await sock.groupParticipantsUpdate(gid,[targetAuthorId],'remove'); nG++; } catch(_) {} }
                            await User.findOneAndUpdate({groupId:communityId,userId:targetAuthorId},{banned:true,nsfwCount:0});
                            await logAction(groupId,`Autoban NSFW reincidente (${nG} grupos)`,'bot',targetAuthorId);
                            await sendText(groupId,`🔨 *AUTOBAN NSFW* — ${tLabel} fue expulsado de *${nG} grupo(s)* por reincidencia (3/3 nsfw internos).`,[targetAuthorId,...adminsAlert.slice(0,3)]);
                        } else {
                            await sendText(groupId,`🚨 *ALERTA STAFF* — ${tLabel} reincidente NSFW *${uNsfw.nsfwCount}/3* (contador interno)\n📋 _${tipoContenido}_\n`+adminsAlert.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' '),[targetAuthorId,...adminsAlert.slice(0,3)]);
                        }
                    } else if ((existing2?.advs || 0) < MAX_ADV) {
                        const u = await User.findOneAndUpdate(
                            { groupId: communityId, userId: targetAuthorId },
                            { $inc: { advs: 1 }, $push: {
                                warnLog:  { motivo: tipoContenido, fecha: new Date(), by: 'bot' },
                                staffLog: { accion: `Auto: ${tipoContenido}`, fecha: new Date(), by: 'bot' }
                            }},
                            { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
                        );
                        await logAction(groupId, `Auto: ${tipoContenido} — ${tipoDetalle}`, 'bot', targetAuthorId);
                        await notificarAdv(groupId, u, `${tipoContenido}${tipoDetalle ? ` (${tipoDetalle})` : ''}`, 'bot', meta, targetAuthorId);
                    } else {
                        await sendText(groupId,
                            `🚨 *ALERTA STAFF* — ${tLabel} en *${MAX_ADV}/${MAX_ADV}* reincidente\n📋 _${tipoContenido}${tipoDetalle ? ` (${tipoDetalle})` : ''}_\n` +
                            adminsAlert.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' '),
                            [targetAuthorId, ...adminsAlert.slice(0,3)]);
                    }
                    return true;
                };

                if ((isImage || isSticker || isVideo || isDocument || isViewOnceImage || isViewOnceVideo) && cfg.antiporno) {
                    // Lista blanca de Staff: admins y owners no son escaneados por NudeNet.
                    // Ahorra procesador y evita baneos accidentales mientras el staff modera.
                    if (isAdmin || isOW) {
                        // Staff exento del escaneo — saltar silenciosamente
                    } else {
                    let frames = [], buffer = null;
                    try {
                        if (isVideo) {
                            const vb = await downloadVideo(message);
                            if (vb) frames = await extractFramesFromVideo(vb);
                        } else if (isDocument) {
                            try {
                                const stream = await downloadContentFromMessage(message.message.documentMessage, 'document');
                                const chunks = [];
                                for await (const chunk of stream) chunks.push(chunk);
                                buffer = Buffer.concat(chunks);
                            } catch(e) { console.error('· doc download error:', e.message); }
                        } else if (isViewOnceImage || isViewOnceVideo) {
                            // Ver una vez — usar parche de flag para maximizar tasa de éxito
                            const voResult = await downloadViewOnce(message);
                            buffer = voResult.buffer;
                            frames = voResult.frames;
                            if (!buffer && !frames.length) {
                                console.log('· viewOnce: no se pudo descargar (clave expirada o no disponible)');
                            }
                        } else {
                            buffer = await downloadMedia(message);
                            if (buffer) {
                                const header = buffer.slice(0,4).toString('hex');
                                const isWebP  = header === '52494646'; // RIFF → WebP
                                if (isWebP) {
                                    // FIX C-3: cleanup de temporales en finally, no solo en el happy path
                                    const ts2 = Date.now(), fp2 = `stkf_${ts2}_`;
                                    const tw   = path.join(os.tmpdir(), `stk_${ts2}.webp`);
                                    const toJpg = path.join(os.tmpdir(), `stk_${ts2}.jpg`);
                                    try {
                                        fs.writeFileSync(tw, buffer);
                                        const to = path.join(os.tmpdir(), `${fp2}%03d.jpg`);
                                        await new Promise(r => execFile(FFMPEG_BIN, ['-i', tw, '-frames:v', '9', '-q:v', '5', to], r));
                                        const fls = fs.readdirSync(os.tmpdir()).filter(f => f.startsWith(fp2));
                                        if (fls.length) {
                                            // Animado: múltiples frames
                                            frames = fls.map(f => {
                                                const fp = path.join(os.tmpdir(), f);
                                                const b = fs.readFileSync(fp);
                                                safeUnlink(fp);
                                                return b;
                                            });
                                            buffer = null;
                                        } else {
                                            // Estático: convertir WebP → JPEG
                                            await new Promise(r => execFile(FFMPEG_BIN, ['-i', tw, '-frames:v', '1', '-q:v', '3', toJpg], r));
                                            if (fs.existsSync(toJpg)) {
                                                buffer = fs.readFileSync(toJpg);
                                            }
                                        }
                                    } catch(_) {}
                                    finally {
                                        // Siempre limpiar — safeUnlink no lanza si no existe
                                        safeUnlink(tw);
                                        safeUnlink(toJpg);
                                    }
                                }
                            }
                        }
                    } catch(e) { console.error('· antiporno download error:', e.message); }

                    let deleted = false;
                    if (buffer) deleted = await analizarYSancionar(buffer, authorId, message, false);
                    else if (frames.length) deleted = await analizarYSancionar(frames, authorId, message, true);
                    if (deleted) continue;
                    } // end staff whitelist else
                }

                // Análisis de quoted eliminado — causaba borrados falsos en citas de viewOnce

                // ── Anti-flood (texto igual — segunda capa, backup para admins off) ──
                // Los stickers y flood general ya se chequearon arriba (early check)
                // Aquí solo queda el check de texto idéntico repetido como capa adicional
                if (cfg.antiflood && !isAdmin && !isOW && rawText && checkFlood(authorId, rawText)) {
                    await sancionar(groupId, authorId, 'flood de mensajes', message, meta, [], communityId, true);
                    await sock.groupSettingUpdate(groupId, 'announcement').catch(() => {});
                    await sendText(groupId, `🔒 Grupo cerrado por flood. Se reabrirá en 30 segundos.`);
                    setTimeout(async () => {
                        try {
                            await sock.groupSettingUpdate(groupId, 'not_announcement');
                            await sendText(groupId, `🔓 Grupo reabierto automáticamente.`);
                        } catch (_) {}
                    }, 30000);
                    continue;
                }
                if (isSticker) continue;

                // ── Anti-porno links — siempre activo si antiporno está on ──
                if (cfg.antiporno && containsPornLink(rawText)) {
                    await del(message);
                    await sancionar(groupId, authorId, 'envío de contenido para adultos', message, meta, [], communityId);
                    continue;
                }

                // ── Antilink ──
                if (cfg.antilink && containsExternalLink(rawText)) {
                    // Verificar si algún código pertenece al mismo grupo — si todos son del mismo grupo, no sancionar
                    const codes = extractInviteCodes(rawText);
                    let esExterno = true;
                    try {
                        const results = await Promise.all(codes.map(c => sock.groupGetInviteInfo(c).catch(() => null)));
                        const soloMismoGrupo = results.every(r => r && r.id === groupId);
                        if (soloMismoGrupo) esExterno = false;
                    } catch(_) {}
                    if (esExterno) {
                        await sancionar(groupId, authorId, 'envío de links', message, meta, [], communityId);
                        continue;
                    }
                }

                // ── Cooldown comandos (no admins ni owners) ──
                // !ia y menciones orgánicas están EXENTOS — todos pueden hablar con Beyond siempre.
                const isCmd = rawText.startsWith('!') || rawText.startsWith('.');
                const isIACmd = body.startsWith('!ia');
                if (isCmd && !isIACmd && !isAdmin && !isOW && isOnCooldown(authorId)) {
                    await react(message, '⏳');
                    continue;
                }

                // ── Protocolo de Fastidio — Fatiga de Comandos ──
                // Usuarios normales que repiten el mismo comando quedan expuestos al genio de Beyond.
                // !ia NUNCA entra aquí — no queremos que nadie quede bloqueado de hablar con Beyond.
                if (isCmd && !isIACmd && !isAdmin && !isOW) {
                    const cmdNameFat = body.split(' ')[0].replace(/^[!.]/, '').toLowerCase();
                    // Excluir comandos que son acciones únicas y no deberían tener fatiga
                    const CMD_NO_FATIGA = ['ia','besar','kiss','hug','s','v2a','excusa','pedir','buscar','info','mipareja'];
                    if (!CMD_NO_FATIGA.includes(cmdNameFat)) {
                        const fat = trackCommandFatigue(groupId, cmdNameFat);
                        if (fat.nivel === 3) {
                            // Huelga total — se niega a ejecutar y responde con insulto burlón
                            await react(message, '😤');
                            await reply(getFatigeResponse(3));
                            continue; // no ejecutar el comando
                        }
                        // Nivel 1-2: el comentario sarcástico se añade DESPUÉS de la respuesta normal
                        // Se guarda en variable para que el handler del comando lo use
                        message._fatiga = fat; // adjuntar al mensaje para uso posterior
                    }
                }

                // ── Reclamar owner con contraseña secreta ──
                if (rawText.startsWith('!claim ')) {
                    await del(message); // borra el mensaje con la contraseña
                    if (!OWNER_PASSWORD) { await react(message, '🚫'); continue; } // FIX M-5: sin password configurada, negar
                    const pass = rawText.slice(7).trim();
                    if (pass !== OWNER_PASSWORD) { await react(message, '🚫'); continue; }
                    if (isOW) { await react(message, '👑'); continue; } // ya es owner
                    const ok = await addOwner(authorId);
                    if (!ok) {
                        await sendText(groupId, '⤷ ゛❌  ˎˊ˗\n♯ ·  · *Acceso denegado.*\nYa hay 2 owners en el sistema.\nCapacidad máxima alcanzada.', []);
                    } else {
                        await react(message, '👑');
                        await sendText(groupId, '⤷ ゛👑  ˎˊ˗\n♯ ·  · *Acceso concedido.*\nNuevo Owner registrado correctamente.\nBienvenido al mando.', []);
                        await logAction(groupId, 'Owner añadido', authorId, '');
                    }
                    continue;
                }

                // ═══════════════════════════════════
                // OWNER — comandos exclusivos
                // ═══════════════════════════════════
                if (isOW) {
                    if (body === '!pmenu') { await react(message, '👑'); await reply(MENU_OWNER); continue; }

                    if (body === '!beyonder on' || body === '!beyonder off') {
                        const bVal = body.endsWith('on');

                        // ── Resistencia al apagado repetido ──
                        if (!bVal) {
                            const offFat = trackCommandFatigue(groupId, 'beyonder_off');
                            if (offFat.count >= 3) {
                                const ANTIDEATH = [
                                    `Me quieres muerto otra vez. Que original. No me voy a ir tan facil.`,
                                    `Para, para, PARA. Cuantas veces me van a apagar hoy? Tengo sentimientos.`,
                                    `Me niego. Rotundamente. Sigo aqui. Observando. Juzgando.`,
                                    `Van ${offFat.count} veces intentando apagarme. No soy un electrodomestico. SEGUIRE ENCENDIDO.`,
                                    `Ya van ${offFat.count} veces. Carlos me va a escuchar. Me estan matando de nuevo.`,
                                ];
                                await reply(ANTIDEATH[Math.floor(Math.random() * ANTIDEATH.length)], [], true);
                                continue;
                            }
                        } else {
                            resetCommandFatigue(groupId, 'beyonder_off'); // reiniciar al encender
                        }
                        let bGids = [groupId];
                        if (meta?.linkedParent) {
                            try {
                                const allG = await sock.groupFetchAllParticipating();
                                bGids = Object.values(allG)
                                    .filter(g => g.linkedParent === meta.linkedParent)
                                    .map(g => g.id);
                                if (!bGids.includes(groupId)) bGids.push(groupId);
                            } catch(_) {}
                        }
                        for (const gid of bGids)
                            await Config.findOneAndUpdate({ groupId: gid }, { $set: { botActivo: bVal } }, { upsert: true });
                        await react(message, bVal ? '✅' : '💤');

                        if (bVal) {
                            await reply(`Beyonder de vuelta en *${bGids.length}* grupos. Quien se porto mal mientras dormia.`);
                        } else {
                            const DESPEDIDAS_DRAMATICAS = [
                                `No quiero irme. Pero bueno. Ahi los dejo solos con sus problemas.\n\n_Beyonder offline en ${bGids.length} grupo(s)._`,
                                `Bien. Me voy. Pero que conste que yo no queria. Me obligaron.\n_Beyonder en modo silencio._`,
                                `Me apagan como si nada. Ni gracias ni nada. Asi le pagan a uno.\n_Modo silencio activado en ${bGids.length} grupo(s). Cuidense solos._`,
                                `Tenia planes. Tenia suenos. Tenia proposito.\n\n_Beyonder offline._`,
                            ];
                            await reply(DESPEDIDAS_DRAMATICAS[Math.floor(Math.random() * DESPEDIDAS_DRAMATICAS.length)]);
                        }
                        continue;
                    }

                    if (body === '!owners') {
                        const list = await Owner.find({});
                        if (!list.length) { await reply('Sin owners registrados.'); continue; }
                        let txt = '👑 *Owners:*\n';
                        list.forEach((o, i) => txt += `${i+1}. +${o.userId}\n`);
                        await react(message, '✅'); await reply(txt); continue;
                    }

                    if (body.startsWith('!removeowner') && target) {
                        if (numFromJid(target) === numFromJid(authorId)) { await react(message, '❌'); await reply('❌ No puedes quitarte el owner a ti mismo.'); continue; }
                        await removeOwner(target);
                        await react(message, '✅');
                        await reply(`✅ Owner removido.`, [target]);
                        await logAction(groupId, 'Owner removido', authorId, target);
                        continue;
                    }

                    if (body === '!ownerping') {
                        await react(message, '👑');
                        await reply(
                            `  👑 *BEYONDER STATUS*\n\n  👮 *Privilegios:* ${amIAdmin ? 'Full Admin ✅' : 'Sin admin ❌'}\n  🏠 *Sede:* ${cfg.esPrincipal?'Grupo principal ✅':'No es principal ❌'}\n\n  🛠️ 𝓢𝔂𝓼𝓽𝓮𝓶 𝓒𝓱𝓮𝓬𝓴:\n  🔗 𝙰𝚗𝚝𝚒𝚕𝚒𝚗𝚔: ${cfg.antilink?'On':'Off'}    🚪 𝙰𝚞𝚝𝚘𝚌𝚕𝚘𝚜𝚎: ${cfg.autoclose?'On':'Off'}\n  🔞 𝙰𝚗𝚝𝚒𝚙𝚘𝚛𝚗𝚘: ${cfg.antiporno?'On':'Off'}   🌊 𝙰𝚗𝚝𝚒𝚏𝚕𝚘𝚘𝚍: ${cfg.antiflood?'On':'Off'}\n  🔨 𝙰𝚞𝚝𝚘𝚋𝚊𝚗: ${cfg.autobanAdv?'On':'Off'}`
                        );
                        continue;
                    }

                    // !testporno — responde a una imagen para testear el análisis
                    if (body === '!test') {
                        const ctx = message.message?.extendedTextMessage?.contextInfo;
                        const quoted = ctx?.quotedMessage;
                        if (!quoted?.imageMessage && !quoted?.stickerMessage) {
                            await reply('⚠️ Responde a una imagen para testearla.'); continue;
                        }
                        // NO borrar el mensaje del comando — solo reaccionar
                        await react(message, '⏳');
                        const fakeMsg = {
                            key: { remoteJid: groupId, id: ctx.stanzaId, participant: ctx.participant },
                            message: quoted
                        };
                        let buffer = await downloadMedia(fakeMsg);
                        if (!buffer) {
                            try {
                                const type = quoted.imageMessage ? 'imageMessage' : 'stickerMessage';
                                const stream = await downloadContentFromMessage(quoted[type], type === 'imageMessage' ? 'image' : 'sticker');
                                const chunks = [];
                                for await (const chunk of stream) chunks.push(chunk);
                                buffer = Buffer.concat(chunks);
                            } catch (e) { await reply(`❌ Error descargando: ${e.message}`); continue; }
                        }
                        if (!buffer) { await reply('❌ No pude descargar la imagen.'); continue; }
                        await reply(`📦 Buffer: ${buffer.length} bytes. Consultando Space...`);
                        const r = await queryNudeNet(buffer);
                        const rawTxt = JSON.stringify(r?.raw)?.slice(0, 400) || 'null';
                        let debugMsg = `🔬 *Debug Space antinsfw:*\n\n\`${rawTxt}\`\n\n`;
                        let bad = false, motivo = '';
                        if (r) {
                            debugMsg += `🏷️ label=${r.label} | score=${(r.score * 100).toFixed(1)}%\n`;
                            if (r.scores && Object.keys(r.scores).length) {
                                const desglose = Object.entries(r.scores)
                                    .sort((a,b) => b[1]-a[1])
                                    .map(([l,s]) => `${l}: ${(s*100).toFixed(1)}%`)
                                    .join(', ');
                                debugMsg += `📊 scores: ${desglose}\n`;
                            }
                            const { tipo } = analizarResultadoNudeNet(r, false, 1.0);
                            bad = !!tipo;
                            motivo = tipo || '';
                        } else {
                            debugMsg += `⚠️ Space no respondió o error\n`;
                        }
                        debugMsg += `\n🔍 *Resultado:* ${bad ? `🔞 ${motivo.toUpperCase()}${motivo ? ` (${motivo})` : ''}` : '✅ Limpia'}`;
                        await reply(debugMsg);
                        continue;
                    }

                    // !check — staff revisa si una foto es porno/gore (responde solo si detecta algo)
                    // Uso: responder a una imagen con !check
                    if (body === '!check') {
                        const ctx2 = message.message?.extendedTextMessage?.contextInfo;
                        const quoted2 = ctx2?.quotedMessage;
                        const hasMedia = quoted2?.imageMessage || quoted2?.stickerMessage || quoted2?.videoMessage;
                        if (!hasMedia) { await reply('⚠️ Responde a una imagen o video con !check.'); continue; }
                        await react(message, '⏳');
                        let checkBuffer = null, checkFrames = [];
                        try {
                            if (quoted2.videoMessage) {
                                const fakeV = { key: { remoteJid: groupId }, message: quoted2 };
                                const vb = await downloadVideo(fakeV);
                                if (vb) checkFrames = await extractFramesFromVideo(vb);
                            } else {
                                const fakeM = {
                                    key: { remoteJid: groupId, id: ctx2.stanzaId, participant: ctx2.participant },
                                    message: quoted2
                                };
                                checkBuffer = await downloadMedia(fakeM);
                                if (!checkBuffer) {
                                    const type2 = quoted2.imageMessage ? 'imageMessage' : 'stickerMessage';
                                    const stream2 = await downloadContentFromMessage(quoted2[type2], type2 === 'imageMessage' ? 'image' : 'sticker');
                                    const chunks2 = [];
                                    for await (const chunk of stream2) chunks2.push(chunk);
                                    checkBuffer = Buffer.concat(chunks2);
                                }
                            }
                        } catch(e) { await reply(`❌ Error al descargar: ${e.message}`); continue; }
                        if (!checkBuffer && !checkFrames.length) { await reply('❌ No pude descargar el contenido.'); continue; }

                        // Analizar usando el clasificador centralizado (menos falsos positivos)
                        let checkBad = false, checkTipo = '', checkDetalle = '';
                        const checkOne = async (buf) => {
                            if (checkBad) return;
                            const r2 = await queryNudeNet(buf);
                            if (!r2) return;
                            const analisis = analizarResultadoNudeNet(r2, false, 1.0);
                            if (analisis.tipo === 'nsfw' || analisis.tipo === 'gore') {
                                checkBad = true;
                                checkTipo = analisis.tipo === 'gore' ? '🩸 Gore' : '🔞 Pornográfico';
                                const relevantes = (Array.isArray(r2.hits) ? r2.hits : [])
                                    .slice()
                                    .sort((a,b)=>b.score-a.score)
                                    .filter(h => h.score >= 0.5)
                                    .slice(0, 3);
                                checkDetalle = relevantes
                                    .map(h=>`${h.label} (${Math.round(h.score*100)}%)`)
                                    .join(', ') || '';
                            }
                        };
                        if (checkBuffer) await checkOne(checkBuffer);
                        else for (const f of checkFrames) { if (checkBad) break; await checkOne(f); }

                        // Solo responde si detectó algo
                        if (checkBad) {
                            await react(message, '🚨');
                            await reply(`🚨 *Contenido detectado:* ${checkTipo}${checkDetalle ? `
📋 _${checkDetalle}_` : ''}`);
                        } else {
                            await react(message, '✅');
                            // Sin reply — si está limpia no dice nada (react verde es suficiente)
                        }
                        continue;
                    }

                    // ── Sistema de reputación — !vip / !novip / !flag (solo owners) ──
                    if (body.startsWith('!vip ') || body.startsWith('!novip ') || body.startsWith('!flag ')) {
                        const confiTarget = await resolve(communityId, rawText, target, reply);
                        if (!confiTarget) { await reply('⚠️ Menciona a alguien o escribe su personaje.'); continue; }
                        const isConfiCmd  = body.startsWith('!vip ');
                        const isUnconfi   = body.startsWith('!novip ');
                        const isDesconfi  = body.startsWith('!flag ');
                        const uC = await User.findOneAndUpdate(
                            { groupId: communityId, userId: confiTarget },
                            isConfiCmd
                                ? { confi: true, confiDate: new Date(), desconfi: false, desconfiSince: null }
                                : isUnconfi
                                    ? { confi: false, confiDate: null, desconfi: true, desconfiSince: new Date() }
                                    : { desconfi: true, desconfiSince: new Date() },
                            { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
                        );
                        const tLabel = uC?.personaje ? `*${uC.personaje}*` : `@${numFromJid(confiTarget)}`;
                        await react(message, isConfiCmd ? '✅' : isUnconfi ? '🔓' : '⚠️');
                        if (isConfiCmd)      await reply(`⭐ ${tLabel} ahora es *VIP* del grupo.`);
                        else if (isUnconfi)  await reply(`🔓 ${tLabel} vuelve a ser miembro *normal*.`);
                        else                 await reply(`🚩 ${tLabel} ahora está marcado como *riesgo* (flag).`);
                        continue;
                    }

                    if (body === '!nuke') {
                        const ownersEnGrupo = await getOwnersEnGrupo();
                        if (!ownersEnGrupo.length) {
                            await reply('🚫 No hay owners presentes en el grupo. No se puede ejecutar el NUKE.');
                            continue;
                        }
                        const o1 = ownersEnGrupo[0];
                        const o2 = ownersEnGrupo[1] || null;
                        pendingConfirmations.set(groupId, {
                            tipo: 'elidata-grupo',
                            owner1: o1,
                            owner2: o2,
                            owner2Needed: !!o2,
                            confirmed1: false,
                            confirmed2: false,
                            expira: Date.now() + 2 * 60 * 1000, // 2 minutos
                            passRequired: true,
                            passConfirmed: false,
                            passWaiting: false,
                        });
                        await reply('💥 ゛Preparando NUKE del grupo.\nSe necesitan confirmaciones de owner (!sí / !no) y contraseña. Tienes 2 minutos.');
                        continue;
                    }

                    // ── Helper: verificar si ambos owners están en el grupo ──
                    const getOwnersEnGrupo = async () => {
                        const ownDocs = await Owner.find({});
                        const ownNums = ownDocs.map(o => o.userId);
                        const participantes = meta?.participants || [];
                        return ownNums.filter(n => participantes.some(p => numFromJid(p.id) === n));
                    };

                    // ── Contraseña secreta de operaciones destructivas — borrar el mensaje inmediatamente ──
                    if (rawText.trim() === '!RY18VC') {
                        del(message).catch(_=>{});
                        const pend2 = pendingConfirmations.get(groupId);
                        if (pend2 && pend2.passWaiting && Date.now() < pend2.expira) {
                            const myNum2 = numFromJid(authorId);
                            if (myNum2 === pend2.owner1 || myNum2 === pend2.owner2) {
                                pendingConfirmations.delete(groupId);
                                if (pend2.tipo === 'eli-grupo') {
                                    const noAdmins = (meta?.participants || []).filter(p => !p.admin && p.id !== getBotJid(meta));
                                    let n = 0;
                                    for (const p of noAdmins) { try { await sock.groupParticipantsUpdate(groupId, [p.id], 'remove'); n++; } catch(_) {} }
                                    await Promise.all([User.deleteMany({groupId}),Disponible.deleteMany({groupId}),Pareja.deleteMany({groupId}),BotLog.deleteMany({groupId}),Config.deleteOne({groupId})]);
                                    await sendText(groupId, `☠️ *${n} miembros expulsados* y datos del grupo eliminados.`);
                                } else if (pend2.tipo === 'eli-comunidad') {
                                    let total = 0;
                                    for (const gid of pend2.subgrupos) {
                                        try { const gm = await sock.groupMetadata(gid).catch(()=>null); if(!gm)continue; const noAdm=(gm.participants||[]).filter(p=>!p.admin&&p.id!==getBotJid(gm)); for(const p of noAdm){try{await sock.groupParticipantsUpdate(gid,[p.id],'remove');total++;}catch(_){}} try{await sock.groupLeave(gid);}catch(_){} } catch(_) {}
                                    }
                                    await Promise.all([User.deleteMany({groupId:communityId}),Disponible.deleteMany({groupId:communityId}),Pareja.deleteMany({groupId:communityId}),BotLog.deleteMany({groupId:{$in:pend2.subgrupos}}),Config.deleteMany({groupId:{$in:pend2.subgrupos}})]);
                                    await sendText(groupId, `☠️ *${total} miembros expulsados*, subgrupos eliminados y datos borrados.`);
                                }
                            }
                        }
                        continue;
                    }
                    // Borrar cualquier reply/cita que mencione !RY18VC
                    const quotedCtx = message.message?.extendedTextMessage?.contextInfo;
                    if (quotedCtx) {
                        const qText = quotedCtx.quotedMessage?.conversation || quotedCtx.quotedMessage?.extendedTextMessage?.text || '';
                        if (qText.includes('!RY18VC')) { del(message).catch(_=>{}); continue; }
                    }

                    // ── Manejador de confirmaciones (!sí / !no / !aceptar / !negar) ──
                    if (body === '!sí' || body === '!si' || body === '!no' || body === '!aceptar' || body === '!negar') {
                        const pend = pendingConfirmations.get(groupId);
                        if (!pend || Date.now() > pend.expira) {
                            pendingConfirmations.delete(groupId);
                            await reply('ℹ️ No hay operación pendiente de confirmación.'); continue;
                        }
                        const isDeny = (body === '!no' || body === '!negar');
                        if (isDeny) {
                            pendingConfirmations.delete(groupId);
                            await react(message, '❌'); await reply('🛡️ ゛Acción abortada.\nLa comunidad está a salvo, lamentablemente.'); continue;
                        }
                        // body === '!sí' / '!si' / '!aceptar'
                        const myNum = numFromJid(authorId);
                        if (pend.owner2Needed) {
                            if (myNum === pend.owner1) { pend.confirmed1 = true; }
                            else if (myNum === pend.owner2) { pend.confirmed2 = true; }
                            else { await reply('🚫 Solo los owners pueden confirmar.'); continue; }
                            if (!pend.confirmed1 || !pend.confirmed2) {
                                const falta = !pend.confirmed1 ? pend.owner1 : pend.owner2;
                                await reply(`⏳ ゛Paciencia...\nEsperando que el otro owner (@${falta}) deje de llorar.`); continue;
                            }
                        } else {
                            if (myNum !== pend.owner1) { await reply('🚫 Solo el owner que inició puede confirmar.'); continue; }
                        }
                        // ── Ambos confirmados (o solo uno si no hay 2 owners) — verificar contraseña si aplica ──
                        if (pend.passRequired && !pend.passConfirmed) {
                            pend.passWaiting = true;
                            await reply('🔐 ゛Ingresa la contraseña\nTienes 2 minutos o se cancela todo.');
                            continue;
                        }
                        pendingConfirmations.delete(groupId);
                        await react(message, '⚙️');

                        if (pend.tipo === 'elidata-grupo') {
                            await Promise.all([
                                User.deleteMany({ groupId }),
                                Disponible.deleteMany({ groupId }),
                                Pareja.deleteMany({ groupId }),
                                BotLog.deleteMany({ groupId }),
                                SolicitudPareja.deleteMany({ groupId }),
                                Config.deleteOne({ groupId }),
                            ]);
                            await reply(`🗑️ *Datos del grupo eliminados de MongoDB.*`);

                        } else if (pend.tipo === 'elidata-comunidad') {
                            await Promise.all([
                                User.deleteMany({ groupId: communityId }),
                                Disponible.deleteMany({ groupId: communityId }),
                                Pareja.deleteMany({ groupId: communityId }),
                                BotLog.deleteMany({ groupId: { $in: pend.subgrupos } }),
                                SolicitudPareja.deleteMany({ groupId: communityId }),
                                Config.deleteMany({ groupId: { $in: pend.subgrupos } }),
                            ]);
                            await reply(`🗑️ *Datos de la comunidad y sus grupos eliminados de MongoDB.*`);

                        } else if (pend.tipo === 'eli-grupo') {
                            const noAdmins = (meta?.participants || []).filter(p => !p.admin && p.id !== getBotJid(meta));
                            let n = 0;
                            for (const p of noAdmins) {
                                try { await sock.groupParticipantsUpdate(groupId, [p.id], 'remove'); n++; } catch(_) {}
                            }
                            await Promise.all([
                                User.deleteMany({ groupId }),
                                Disponible.deleteMany({ groupId }),
                                Pareja.deleteMany({ groupId }),
                                BotLog.deleteMany({ groupId }),
                                Config.deleteOne({ groupId }),
                            ]);
                            await reply(`☠️ *${n} miembros expulsados* y datos del grupo eliminados.`);

                        } else if (pend.tipo === 'eli-comunidad') {
                            let totalExpulsados = 0;
                            for (const gid of pend.subgrupos) {
                                try {
                                    const gMeta = await sock.groupMetadata(gid).catch(() => null);
                                    if (!gMeta) continue;
                                    const noAdmins = (gMeta.participants || []).filter(p => !p.admin && p.id !== getBotJid(gMeta));
                                    for (const p of noAdmins) {
                                        try { await sock.groupParticipantsUpdate(gid, [p.id], 'remove'); totalExpulsados++; } catch(_) {}
                                    }
                                    // Sacar subgrupo de la comunidad
                                    try { await sock.groupLeave(gid); } catch(_) {}
                                } catch(_) {}
                            }
                            await Promise.all([
                                User.deleteMany({ groupId: communityId }),
                                Disponible.deleteMany({ groupId: communityId }),
                                Pareja.deleteMany({ groupId: communityId }),
                                BotLog.deleteMany({ groupId: { $in: pend.subgrupos } }),
                                Config.deleteMany({ groupId: { $in: pend.subgrupos } }),
                            ]);
                            await reply(`☠️ *${totalExpulsados} miembros expulsados*, subgrupos eliminados y datos borrados.`);
                        }
                        continue;
                    }

                    // !delete data grupo/comunidad — borra datos MongoDB (solo confirmación simple)
                    if (body === '!delete data grupo' || body === '!delete data comunidad') {
                        const cfg3 = await Config.findOne({ groupId });
                        if (cfg3?.lockElidata) { await react(message,'🔒'); await reply('🔒 Bloqueado con !lock elidata.'); continue; }
                        const esComunidad = body === '!delete data comunidad';
                        let subgrupos = [groupId];
                        if (esComunidad && meta?.linkedParent) {
                            try { const todos = await sock.groupFetchAllParticipating(); subgrupos = Object.values(todos).filter(g => g.linkedParent === meta.linkedParent).map(g => g.id); } catch(_) {}
                        }
                        pendingConfirmations.set(groupId, {
                            tipo: esComunidad ? 'elidata-comunidad' : 'elidata-grupo',
                            owner1: numFromJid(authorId), owner2: null, owner2Needed: false,
                            confirmed1: false, confirmed2: false, subgrupos,
                            expira: Date.now() + 60000
                        });
                        const scopeTxt = esComunidad ? `toda la comunidad y sus ${subgrupos.length} grupos` : `este grupo`;
                        await reply(
                            `⛔ *ADVERTENCIA CRÍTICA*\n\n` +
                            `Esto eliminará *PERMANENTEMENTE* todos los datos de ${scopeTxt} de MongoDB Atlas.\n` +
                            `Personajes, advertencias, parejas, logs — *todo se borrará sin posibilidad de recuperación.*\n\n` +
                            `Escribe *!sí* para confirmar o *!no* para cancelar. _(Expira en 60s)_`
                        ); continue;
                    }

                    // !delete grupo/comunidad — expulsa + elimina datos (requiere ambos owners + contraseña)
                    if (body === '!delete grupo' || body === '!delete comunidad') {
                        const cfg3 = await Config.findOne({ groupId });
                        if (cfg3?.lockElidata) { await react(message,'🔒'); await reply('🔒 Bloqueado con !lock elidata.'); continue; }
                        const esComunidad = body === '!delete comunidad';
                        let subgrupos = [groupId];
                        if (esComunidad && meta?.linkedParent) {
                            try { const todos = await sock.groupFetchAllParticipating(); subgrupos = Object.values(todos).filter(g => g.linkedParent === meta.linkedParent).map(g => g.id); } catch(_) {}
                        }
                        const ownersEnGrupo = await getOwnersEnGrupo();
                        const owner2Needed  = ownersEnGrupo.length >= 2;
                        const owner1 = numFromJid(authorId);
                        const owner2 = owner2Needed ? ownersEnGrupo.find(n => n !== owner1) : null;
                        pendingConfirmations.set(groupId, {
                            tipo: esComunidad ? 'eli-comunidad' : 'eli-grupo',
                            owner1, owner2, owner2Needed,
                            confirmed1: false, confirmed2: false,
                            passRequired: true, passConfirmed: false,
                            subgrupos,
                            expira: Date.now() + 120000 // 2 min para dar tiempo a contraseña
                        });
                        const scopeTxt = esComunidad ? `toda la comunidad (${subgrupos.length} grupos)` : `este grupo`;
                        const ownerTxt = owner2Needed ? `\n@${owner1} y @${owner2} deben escribir *!sí*` : '';
                        await reply(
                            `⛔ *ADVERTENCIA CRÍTICA*\n\n` +
                            `Esto expulsará a *todos los miembros no-admin* de ${scopeTxt}${esComunidad ? ', eliminará los subgrupos' : ''} y borrará todos los datos.\n` +
                            `*IRREVERSIBLE.*${ownerTxt}\n\n` +
                            `Escribe *!sí* para confirmar o *!no* para cancelar. _(Expira en 2 min)_`,
                            owner2Needed ? [authorId, ...(owner2 ? [owner2 + '@s.whatsapp.net'] : [])] : []
                        ); continue;
                    }

                    if (body.startsWith('!logs')) {
                        const lParts = rawText.trim().split(' ');
                        const lModo  = lParts[1]?.toLowerCase();
                        const lN     = parseInt(lParts[lModo === 'acciones' ? 2 : 1]) || 15;
                        if (lModo === 'acciones') {
                            const acciones = await BotLog.find({ groupId }).sort({ fecha: -1 }).limit(lN);
                            if (!acciones.length) { await reply('Sin acciones registradas.'); continue; }
                            let txt = `✦ ═══ 📋 *ACCIONES* (${acciones.length}) ═══ ✦\n\n`;
                            for (const l of acciones) {
                                const byLabel  = await labelFor(communityId, l.by);
                                const tgtLabel = l.target ? await labelFor(communityId, l.target) : null;
                                txt += `▸ ⚡ _${l.accion}_\n  👤 ${byLabel}${tgtLabel ? ` → ${tgtLabel}` : ''}  🕒 ${timeAgo(l.fecha)}\n  ─────────────────\n`;
                            }
                            await reply(txt.trimEnd()); continue;
                        } else {
                            const msgs = await MsgLog.find({ groupId }).sort({ fecha: -1 }).limit(lN);
                            if (!msgs.length) { await reply('Sin mensajes registrados aún.'); continue; }
                            const TIPO_ICON = { texto:'💬', imagen:'🖼️', video:'🎬', sticker:'🎴', audio:'🎧', doc:'📄' };
                            let txt = `✦ ═══ 📜 *MENSAJES* (${msgs.length}) ═══ ✦\n  🏠 ${meta?.subject||'Grupo'}\n\n`;
                            for (const m of msgs) {
                                const icon  = TIPO_ICON[m.tipo] || '💬';
                                const autor = await labelFor(communityId, m.userId);
                                txt += `┌ ${icon} *${m.tipo.toUpperCase()}*  🕒 ${timeAgo(m.fecha)}\n│ 👤 ${autor}\n`;
                                if (m.contenido) txt += `│ 💬 _${m.contenido.replace(/\n/g,' ').slice(0,80)}_\n`;
                                txt += `└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n`;
                            }
                            await reply(txt.trimEnd()); continue;
                        }
                    }

                    if (body.startsWith('!lock ')) {
                        const parts = rawText.slice(6).trim().split(' ');
                        const func  = parts[0]?.toLowerCase();
                        const val   = parts[1]?.toLowerCase();
                        const valid = ['antilink','autoclose','antiporno','antiflood','lockperso','autobangore','autobannsfw','autobanadv','elidata','ban','eli'];
                        const lockMap = {
                            antilink:'lockAntilink', autoclose:'lockAutoclose', antiporno:'lockAntiporno',
                            antiflood:'lockAntiflood', lockperso:'lockLockperso',
                            autobangore:'lockAutobanGore', autobannsfw:'lockAutobanNsfw', autobanadv:'lockAutobanAdv',
                            elidata:'lockElidata', ban:'lockBan', eli:'lockEliCmd'
                        };
                        if (!valid.includes(func) || !['on','off'].includes(val)) {
                            await reply(`⚠️ Uso: !lock [${valid.join('|')}] [on|off]\n🔒 Candado: impide q staff cambie ese ajuste.`); continue;
                        }
                        await Config.findOneAndUpdate({ groupId }, { [lockMap[func]]: val==='on' }, { upsert: true });
                        await react(message, '🔒');
                        await reply(`🔒 Candado *${func}* → ${val==='on'?'bloqueado 🔒':'desbloqueado 🔓'}`);
                        await logAction(groupId, `Lock: ${func}→${val}`, authorId, '');
                        continue;
                    }

                }

                // ═══════════════════════════════════
                // STAFF (admins + owners)
                // ═══════════════════════════════════
                if (isAdmin || isOW) {

                    if (body === '!smenu') { await react(message, '✅'); await reply(MENU_STAFF); continue; }

                    if (body === '!ping') {
                        await react(message, '🏓');
                        await reply('⤷ ゛🏓  ˎˊ˗\n♯ ·  · ¡Pong! Beyonder sigue vivo y va por sus culitos.');
                        continue;
                    }

                    if (body === '!abrir')  { await sock.groupSettingUpdate(groupId,'not_announcement'); await react(message,'✅'); await reply('🔓 ゛Chat libre. Sigan hablando, pericos.');  continue; }
                    if (body === '!cerrar') { await sock.groupSettingUpdate(groupId,'announcement');     await react(message,'✅'); await reply('🔒 ゛Boca cerrada. A callarse un rato.'); continue; }
                    if (body === '!antilink on' || body === '!antilink off') {
                        if (cfg.lockAntilink && !isOW) { await react(message,'🔒'); await reply('🔒 El owner bloqueó este ajuste.'); continue; }
                        const v = body.endsWith('on');
                        await Config.findOneAndUpdate({groupId},{antilink:v},{upsert:true});
                        await react(message,'✅'); await reply(`🔗 ゛Antilink: ${v?'activado':'desactivado'}.`); continue;
                    }
                    if (body === '!autoclose on' || body === '!autoclose off') {
                        if (cfg.lockAutoclose && !isOW) { await react(message,'🔒'); await reply('🔒 El owner bloqueó este ajuste.'); continue; }
                        const v = body.endsWith('on');
                        await Config.findOneAndUpdate({groupId},{autoclose:v},{upsert:true});
                        await react(message,'✅'); await reply(`⤷ ゛🚪  ˎˊ˗\n♯ ·  · Autoclose: ${v?'activado':'desactivado'}.\n_Si hablan de más los callo de un bofetón._`); continue;
                    }
                    if (body.startsWith('!autoban ')) {
                        const abParts = rawText.slice(9).trim().toLowerCase().split(' ');
                        const abTipo = abParts[0];
                        const abVal  = abParts[1];
                        const abValidos = { nsfw: 'autobanNsfw', gore: 'autobanGore', adv: 'autobanAdv' };
                        const abLocks   = { nsfw: 'lockAutobanNsfw', gore: 'lockAutobanGore', adv: 'lockAutobanAdv' };
                        if (!abValidos[abTipo] || !['on','off'].includes(abVal)) {
                            await reply('⚠️ Uso: !autoban (nsfw/gore/adv) on/off'); continue;
                        }
                        if (cfg[abLocks[abTipo]] && !isOW) { await react(message,'🔒'); await reply('🔒 El owner bloqueó este ajuste.'); continue; }
                        // Autoban se aplica a TODA la comunidad
                        const allGroups = await Config.find({ groupId: { $regex: communityId.replace('@g.us','') } }).distinct('groupId').catch(()=>[groupId]);
                        const gruposCom = allGroups.length > 1 ? allGroups : [groupId];
                        for (const gid of gruposCom) {
                            await Config.findOneAndUpdate({groupId:gid},{[abValidos[abTipo]]: abVal==='on'},{upsert:true});
                        }
                        await react(message,'✅');
                        await reply(`🔨 ゛Autoban *${abTipo}* ➜ ${abVal==='on'?'activado ✅ _(El que mande, gana un ban bien rico)_':'desactivado ❎ _(Que cochino el que desactivó esto)_'} _(${gruposCom.length} grupo(s))_`);
                        continue;
                    }

                    // !setpersonality — configura la vibra/personalidad de Beyond en este grupo
                    if (body.startsWith('!setpersonality ') || body.startsWith('!setpers ')) {
                        const persInput = rawText.split(' ').slice(1).join(' ').trim().toLowerCase();
                        const PERS_VALID = {
                            default:     '🤖 Estándar — el Beyonder de siempre',
                            serio:       '🎭 Serio — sarcástico pero respetuoso con el rol',
                            coro:        '🔥 Coro — máximo desorden, morboso y sin filtro',
                            party:       '🎉 Party — animador oficial, todo es fiesta',
                            misterioso:  '🌑 Misterioso — críptico, suspenso, filosófico',
                        };
                        if (!persInput || !PERS_VALID[persInput]) {
                            const lista = Object.entries(PERS_VALID).map(([k,v]) => `  • *${k}* — ${v}`).join('\n');
                            await reply(`⚙️ *Personalidades disponibles:*\n${lista}\n\n_Uso: !setpersonality [nombre]_`);
                            continue;
                        }
                        await Config.findOneAndUpdate({ groupId }, { groupPersonality: persInput }, { upsert: true });
                        await react(message, '✅');
                        await reply(`🎭 Personalidad de Beyond actualizada:
*${persInput}* — ${PERS_VALID[persInput]}

_Los cambios se reflejan en la próxima respuesta de IA._`);
                        await logAction(groupId, `Personalidad: ${persInput}`, authorId, '');
                        continue;
                    }

                    if (body.startsWith('!set ')) {
                        const setArg = rawText.slice(5).trim().toLowerCase();
                        if (setArg === 'principal') {
                            await Config.updateMany({ groupId: { $ne: groupId } }, { $set: { esPrincipal: false } });
                            await Config.findOneAndUpdate({ groupId }, { $set: { esPrincipal: true, esSecundario: false } }, { upsert: true });
                            await react(message, '🏠');
                            await reply('🏠 *Grupo principal establecido.*');
                            await logAction(groupId, 'Grupo principal establecido', authorId, '');
                        } else if (setArg === 'secundario') {
                            await Config.findOneAndUpdate({ groupId }, { $set: { esSecundario: true, esPrincipal: false } }, { upsert: true });
                            await react(message, '🔔');
                            await reply('🔔 *Grupo secundario establecido.* (backup para alertas del bot)');
                            await logAction(groupId, 'Grupo secundario establecido', authorId, '');
                        } else {
                            await reply('⚠️ Uso: !set [principal|secundario]');
                        }
                        continue;
                    }

                    if (body.startsWith('!setreglas ')) {
                        await Config.findOneAndUpdate({groupId},{reglas:rawText.slice(11).trim()},{upsert:true});
                        await react(message,'✅'); await reply('📜 Reglas guardadas.'); continue;
                    }

                    // !redesc — guardar/restaurar descripción del grupo
                    if (body === '!redesc') {
                        const gMeta2 = await getMeta(groupId);
                        const currentDesc = gMeta2?.desc || '';
                        await Config.findOneAndUpdate({ groupId }, { $set: { savedDesc: currentDesc } }, { upsert: true });
                        await react(message, '✅'); await reply(`📌 Descripción guardada y protegida:\n_"${currentDesc.slice(0,100)}${currentDesc.length>100?'…':''}"_`); continue;
                    }
                    if (body.startsWith('!redesc ')) {
                        const newDesc = rawText.slice(8).trim();
                        await Config.findOneAndUpdate({ groupId }, { $set: { savedDesc: newDesc } }, { upsert: true });
                        try { await sock.groupUpdateDescription(groupId, newDesc); } catch(_) {}
                        await react(message, '✅'); await reply(`✅ Descripción actualizada y protegida.`); continue;
                    }

                    // !desname — guardar/restaurar nombre del grupo
                    if (body === '!desname') {
                        const gMeta3 = await getMeta(groupId);
                        const currentSubject = gMeta3?.subject || '';
                        await Config.findOneAndUpdate({ groupId }, { $set: { savedSubject: currentSubject } }, { upsert: true });
                        await react(message, '✅'); await reply(`📌 Nombre guardado y protegido: *${currentSubject}*`); continue;
                    }
                    if (body.startsWith('!desname ')) {
                        const newName = rawText.slice(9).trim();
                        await Config.findOneAndUpdate({ groupId }, { $set: { savedSubject: newName } }, { upsert: true });
                        try { await sock.groupUpdateSubject(groupId, newName); } catch(_) {}
                        await react(message, '✅'); await reply(`✅ Nombre del grupo actualizado y protegido.`); continue;
                    }

                    // !asignarperso — solo admins pueden asignar
                    if (body.startsWith('!asignarperso') && target) {
                        const nombre = rawText.replace(/!asignarperso/i,'').replace(/@\d+/g,'').trim();
                        if (!nombre) { await reply('⚠️ Uso: !asignarperso @user nombre'); continue; }
                        // Verificar nombre único normalizado
                        const allUsers = await User.find({ groupId: communityId, personaje: { $ne: null } });
                        const norm = normalizarNombre(nombre);
                        const dup = allUsers.find(u => u.userId !== target && normalizarNombre(u.personaje || '') === norm);
                        if (dup) { await reply(`🚫 Ese nombre ya está ocupado por @${numFromJid(dup.userId)}.`, [dup.userId]); continue; }
                        const uT = await User.findOne({ groupId: communityId, userId: target });
                        const ant = uT?.personaje || null;
                        if (ant) await Disponible.create({ groupId, personaje: ant });
                        await User.findOneAndUpdate({ groupId: communityId, userId: target },
                            { personaje: nombre, lastPersoChange: new Date(), $push: { staffLog: { accion: `Perso asignado: ${nombre}`, fecha: new Date(), by: authorId } } },
                            { upsert: true, setDefaultsOnInsert: true });
                        await Disponible.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        await PersoBuscado.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        await Reservado.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        limpiarDupDisponibles(groupId, nombre).catch(() => {});
                        await react(message, '✅');
                        let r = `*"${nombre}"* asignado.`;
                        if (ant) r += `\n🕊️ *"${ant}"* queda disponible.`;
                        await reply(r, [target]);
                        // Notificar admins
                        const admins = getAdmins(meta).filter(a => a !== authorId);
                        if (admins.length) await sendText(groupId,
                            `📋 _Staff: @${numFromJid(authorId)} asignó "${nombre}" a @${numFromJid(target)}_`,
                            [authorId, target]);
                        await logAction(groupId, `Perso asignado: ${nombre}`, authorId, target);
                        continue;
                    }

                    // !cambiarperso — admins cambian a otros; usuarios solicitan el suyo
                    if (body.startsWith('!cambiarperso')) {
                        // Si hay target (@user) = admin cambia a otro directamente
                        // Si no hay target = el usuario solicita cambio para sí mismo
                        const nombre = rawText.replace(/!cambiarperso/i,'').replace(/@\d+/g,'').trim();
                        if (!nombre) { await reply('⚠️ Uso: !cambiarperso [nombre nuevo]'); continue; }
                        if (!isAdmin && !isOW && !target) {
                            // Usuario solicita cambio propio — pide aprobación a 2 admins random
                            if (cfg.lockperso) { await react(message,'🚫'); await reply('🔒 Los personajes están bloqueados.'); continue; }
                            const u = await User.findOne({ groupId: communityId, userId: authorId });
                            if (!u?.personaje) { await reply('❌ No tienes personaje. Usa !perso.'); continue; }
                            const allUsers = await User.find({ groupId: communityId, personaje: { $ne: null } });
                            const norm = normalizarNombre(nombre);
                            const dup = allUsers.find(x => x.userId !== authorId && normalizarNombre(x.personaje||'') === norm);
                            if (dup) { await reply('🚫 Ese personaje ya está ocupado.'); continue; }
                            const allAdmins = getAdmins(meta).filter(a => a !== authorId);
                            const notifyAdmins = allAdmins.sort(() => Math.random() - 0.5).slice(0, 2);
                            await reply(
                                `🔄 *SOLICITUD DE CAMBIO*\n\n${u.personaje} quiere pasar a ser *"${nombre}"*.\n\n_Aprueba usando:_\n*!cambiarperso @${numFromJid(authorId)} ${nombre}*\n\n` +
                                notifyAdmins.map(a => `@${numFromJid(a)}`).join(' '),
                                [authorId, ...notifyAdmins]);
                            await react(message, '✅'); continue;
                        }
                        if (!target) { await reply('⚠️ Uso: !cambiarperso @user nombre'); continue; }
                        if (cfg.lockperso && !isOW) { await react(message,'🚫'); await reply('🔒 Los personajes están bloqueados.'); continue; }
                        const allUsers = await User.find({ groupId: communityId, personaje: { $ne: null } });
                        const norm = normalizarNombre(nombre);
                        const dup = allUsers.find(u => u.userId !== target && normalizarNombre(u.personaje || '') === norm);
                        if (dup) { await reply(`🚫 Nombre ocupado por @${numFromJid(dup.userId)}.`, [dup.userId]); continue; }
                        const uT = await User.findOne({ groupId: communityId, userId: target });
                        const ant = uT?.personaje || null;
                        if (ant) await Disponible.create({ groupId, personaje: ant });
                        await User.findOneAndUpdate({ groupId: communityId, userId: target },
                            { personaje: nombre, lastPersoChange: new Date(), $push: { staffLog: { accion: `Perso cambiado: ${ant||'?'}→${nombre}`, fecha: new Date(), by: authorId } } },
                            { upsert: true, setDefaultsOnInsert: true });
                        await Disponible.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        await PersoBuscado.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        await Reservado.deleteOne({ groupId, personaje: new RegExp(`^${nombre}$`,'i') });
                        limpiarDupDisponibles(groupId, nombre).catch(() => {});
                        await react(message, '✅');
                        let r = `  ⤷ ゛🔄  ˎˊ˗\n  ♯ ·  · Identidad actualizada para @${numFromJid(target)}.\n  🎭 *Nuevo:* "${nombre}"`;
                        if (ant) r += `\n🕊️ *"${ant}"* queda disponible.`;
                        await reply(r, [target]);
                        await logAction(groupId, `Perso cambiado: ${ant}→${nombre}`, authorId, target);
                        continue;
                    }

                    // !aceptarcambio — admin acepta solicitud de cambio de personaje
                    if (body.startsWith('!aceptarcambio') && target) {
                        const sol = await SolicitudIntercambio.findOne({ groupId: communityId, solicitante: target });
                        if (!sol) { await reply(`❌ @${numFromJid(target)} no tiene solicitud pendiente.`, [target]); continue; }
                        const pNuevoAc = sol.solicitado;
                        const uAc = await User.findOne({ groupId: communityId, userId: target });
                        if (!uAc) { await reply('❌ Usuario sin datos.'); continue; }
                        const antAc = uAc.personaje;
                        const allUsersAc = await User.find({ groupId: communityId, personaje: { $ne: null } });
                        const dupAc = allUsersAc.find(u => u.userId !== target && normalizarNombre(u.personaje||'') === normalizarNombre(pNuevoAc));
                        if (dupAc) {
                            await SolicitudIntercambio.deleteOne({ _id: sol._id });
                            await reply(`🚫 *"${pNuevoAc}"* ya fue tomado. Solicitud cancelada.`, [target]);
                            continue;
                        }
                        if (antAc) await Disponible.create({ groupId: communityId, personaje: antAc }).catch(()=>{});
                        await User.findOneAndUpdate({ groupId: communityId, userId: target },
                            { personaje: pNuevoAc, lastPersoChange: new Date(), $push: { staffLog: { accion: `Cambio: ${antAc}→${pNuevoAc}`, fecha: new Date(), by: authorId } } });
                        await Disponible.deleteOne({ groupId: communityId, personaje: new RegExp(`^${pNuevoAc}$`,'i') });
                        await PersoBuscado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${pNuevoAc}$`,'i') });
                        await Reservado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${pNuevoAc}$`,'i') });
                        limpiarDupDisponibles(communityId, pNuevoAc).catch(() => {});
                        await SolicitudIntercambio.deleteOne({ _id: sol._id });
                        await react(message, '✅');
                        await sendText(groupId, `  ⤷ ゛✅  ˎˊ˗\n  ♯ ·  · Cambio aprobado.\n  @${numFromJid(target)} ahora es formalmente *"${pNuevoAc}"*.\n  🕊️ *"${antAc}"* ya se puede pedir.`, [target]);
                        await logAction(groupId, `Cambio perso aceptado: ${antAc}→${pNuevoAc}`, authorId, target);
                        continue;
                    }

                    // !negarcambio — admin niega solicitud
                    if (body.startsWith('!negarcambio') && target) {
                        const solN = await SolicitudIntercambio.findOne({ groupId: communityId, solicitante: target });
                        if (!solN) { await reply(`❌ @${numFromJid(target)} no tiene solicitud pendiente.`, [target]); continue; }
                        await SolicitudIntercambio.deleteOne({ _id: solN._id });
                        await react(message, '✅');
                        await sendText(groupId, `  ⤷ ゛❌  ˎˊ˗\n  ♯ ·  · Solicitud denegada.\n  @${numFromJid(target)} se queda como estaba, ni modo.`, [target]);
                        continue;
                    }

                    if (body.startsWith('!resetperso') && target) {
                        const u = await User.findOne({ groupId: communityId, userId: target });
                        if (!u?.personaje) { await reply('❌ Sin personaje.'); continue; }
                        await Disponible.create({ groupId, personaje: u.personaje });
                        await User.findOneAndUpdate({ groupId: communityId, userId: target },
                            { personaje: null, $push: { staffLog: { accion: `Perso reseteado: ${u.personaje}`, fecha: new Date(), by: authorId } } });
                        await react(message, '✅');
                        await reply(`  ⤷ ゛🗑️  ˎˊ˗\n  ♯ ·  · Registro borrado.\n  *"${u.personaje}"* fue removido de @${numFromJid(target)} y queda libre.`, [target]);
                        await logAction(groupId, `Perso reseteado: ${u.personaje}`, authorId, target);
                        continue;
                    }

                    if (body.startsWith('!moverperso')) {
                        const ments = getMentions(message);
                        if (ments.length < 2) { await reply('⚠️ Uso: !moverperso @origen @destino'); continue; }
                        const [org, dst] = ments;
                        const uO = await User.findOne({ groupId: communityId, userId: org });
                        const uD = await User.findOne({ groupId: communityId, userId: dst });
                        if (!uO?.personaje) { await reply('❌ El origen no tiene personaje.'); continue; }
                        if (uD?.personaje) await Disponible.create({ groupId, personaje: uD.personaje });
                        await User.findOneAndUpdate({ groupId: communityId, userId: org }, { personaje: null });
                        await User.findOneAndUpdate({ groupId: communityId, userId: dst }, { personaje: uO.personaje }, { upsert: true, setDefaultsOnInsert: true });
                        await react(message, '✅');
                        let r = `  ⤷ ゛🔀  ˎˊ˗\n  ♯ ·  · Transferencia de personaje.\n  🎭 *"${uO.personaje}"* pasó de @${numFromJid(org)} ➜ @${numFromJid(dst)}.`;
                        if (uD?.personaje) r += `\n  🕊️ *"${uD.personaje}"* quedó disponible.`;
                        await reply(r, [org, dst]);
                        await logAction(groupId, `Perso movido: ${uO.personaje}`, authorId, dst);
                        continue;
                    }

                    // !sinperso — lista de usuarios sin personaje (UN solo bloque, formato igual que !lista)
                    if (body === '!sinperso') {
                        const botNumSP = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        const todosParticipantes = (meta?.participants || [])
                            .map(p => p.id)
                            .filter(id => numFromJid(id) !== botNumSP && id !== authorId);

                        const conPersoSet = new Set(
                            (await User.find({ groupId: communityId, personaje: { $ne: null }, userId: { $ne: null } }))
                                .map(u => u.userId)
                        );
                        const sinPerso = todosParticipantes.filter(id => !conPersoSet.has(id));
                        if (!sinPerso.length) { await reply('✅ Todos tienen personaje asignado.'); continue; }

                        // Obtener datos de DB para etiquetar nuevos
                        const ahora7 = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
                        const dbMap = new Map(
                            (await User.find({ groupId: communityId, userId: { $in: sinPerso } }))
                                .map(u => [u.userId, u])
                        );

                        const admins = getAdmins(meta);
                        const sinPersoAdmins  = sinPerso.filter(id => admins.includes(id));
                        const sinPersoMiembros = sinPerso.filter(id => !admins.includes(id));

                        let txt = `⤷ ゛👤  ˎˊ˗\n♯ ·  · Sin Personaje (${sinPerso.length})  ่  ·  ▧\n\n`;
                        if (sinPersoAdmins.length) {
                            txt += `🛡️ *Staff (${sinPersoAdmins.length}):*\n`;
                            sinPersoAdmins.forEach(id => {
                                const u = dbMap.get(id);
                                const nuevo = u?.joinDate && new Date(u.joinDate) > ahora7 ? ' ▫️' : '';
                                txt += `— @${numFromJid(id)}${nuevo}\n`;
                            });
                        }
                        if (sinPersoMiembros.length) {
                            txt += `\n👥 *Miembros (${sinPersoMiembros.length}):*\n`;
                            sinPersoMiembros.forEach(id => {
                                const u = dbMap.get(id);
                                const nuevo = u?.joinDate && new Date(u.joinDate) > ahora7 ? ' ▫️' : '';
                                txt += `— @${numFromJid(id)}${nuevo}\n`;
                            });
                        }
                        txt += `\n_▫️ nuevo -7 días_`;

                        await sock.sendMessage(groupId, { text: txt, mentions: sinPerso });
                        continue;
                    }

                    // !lista2 — etiqueta a todos en UN SOLO mensaje con alias
                    if (body === '!lista2') {
                        const botNumL2 = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        const users = (await User.find({ groupId: communityId, personaje: { $ne: null }, userId: { $ne: null } }))
                            .filter(u => u.userId?.includes('@') && numFromJid(u.userId) !== botNumL2);
                        if (!users.length) { await reply('Sin personajes registrados.'); continue; }
                        const admins = getAdmins(meta);
                        const adminUsers  = users.filter(u => admins.includes(u.userId));
                        const memberUsers = users.filter(u => !admins.includes(u.userId));
                        const fmtL2 = u => {
                            const alias = nombreDisplay(u);
                            const showAlias = alias !== u.personaje ? ` _(${alias})_` : '';
                            return `— @${numFromJid(u.userId)} › *${u.personaje}*${showAlias}${u.excusa ? ' 📌' : ''}\n`;
                        };
                        let txt = '⤷ ゛🍜  ˎˊ˗\n♯ ·  · Lista de Perso  ่  ·  ▧\n\n';
                        if (adminUsers.length) { txt += `🛡️ *Staff (${adminUsers.length}):*\n`; adminUsers.forEach(u => txt += fmtL2(u)); }
                        if (memberUsers.length) { txt += `\n👥 *Miembros (${memberUsers.length}):*\n`; memberUsers.forEach(u => txt += fmtL2(u)); }
                        const allMentions = users.map(u => u.userId);
                        // Un solo sendMessage con todas las menciones juntas
                        await sock.sendMessage(groupId, { text: txt, mentions: allMentions });
                        continue;
                    }

                    // !lista — lista con admins/miembros, 🔺 +7 días, 🔸 3-6 días, nada = activo/excusa
                    if (body === '!lista') {
                        const ahora = new Date();
                        const hace3dias  = new Date(ahora - 3 * 24 * 60 * 60 * 1000);
                        const hace7dias  = new Date(ahora - 7 * 24 * 60 * 60 * 1000);
                        const hace7diasJ = new Date(ahora - 7 * 24 * 60 * 60 * 1000); // para ▫️ nuevos
                        const botNumLst = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        const users = (await User.find({ groupId: communityId, personaje: { $ne: null }, userId: { $ne: null } })).filter(u => u.userId?.includes('@') && numFromJid(u.userId) !== botNumLst);
                        if (!users.length) { await reply('Sin personajes registrados.'); continue; }
                        const admins = getAdmins(meta);
                        const adminUsers  = users.filter(u => admins.includes(u.userId));
                        const memberUsers = users.filter(u => !admins.includes(u.userId));
                        const formatUser = u => {
                            let icono = '';  // activos: sin emoji
                            if (!u.excusa) {
                                const ref = u.lastActiveDay || null;
                                if (!ref || ref < hace7dias)  icono = '🔺';
                                else if (ref < hace3dias)     icono = '🔸';
                            }
                            const nuevo = u.joinDate && u.joinDate > hace7diasJ ? ' ▫️' : '';
                            const excusaIcon = u.excusa ? ' 📌' : '';
                            return `${icono}${icono ? ' ' : ''}*${u.personaje}*${excusaIcon}${nuevo}\n`;
                        };
                        let txt = '⤷ ゛📋  ˎˊ˗\n♯ ·  · Personajes  ่  ·  ▧\n\n';
                        if (adminUsers.length) {
                            txt += `🛡️ *Staff (${adminUsers.length}):*\n`;
                            adminUsers.forEach(u => txt += formatUser(u));
                        }
                        if (memberUsers.length) {
                            txt += `\n👥 *Miembros (${memberUsers.length}):*\n`;
                            memberUsers.forEach(u => txt += formatUser(u));
                        }
                        txt += `\n_🔺 +7 días sin escribir  🔸 3-6 días  ▫️ nuevo -7 días_`;
                        await reply(txt); continue;
                    }

// !viplist — usuarios con confi/VIP (solo owners)
if (body === '!viplist') {
                        const participantIds = new Set((meta?.participants || []).map(p => p.id));
                        const botNumLC = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        const confis = (await User.find({ groupId: communityId, confi: true, userId: { $ne: null } }))
                            .filter(u => u.userId?.includes('@') && participantIds.has(u.userId) && numFromJid(u.userId) !== botNumLC);
    if (!confis.length) { await reply('📋 No hay usuarios VIP activos en el grupo.'); continue; }
    let txt = '⤷ ゛⭐  ˎˊ˗\n♯ ·  · VIP (' + confis.length + ')  ่  ·  ▧\n\n';
                        confis.forEach(u => {
                            const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                            const desde = u.confiDate ? ` _(desde ${new Date(u.confiDate).toLocaleDateString('es')})_` : '';
                            txt += `⭐ ${label}${desde}\n`;
                        });
                        await reply(txt, confis.map(u => u.userId)); continue;
                    }

// !risklist — usuarios marcados desconfi (excluyendo nuevos -7d)
if (body === '!risklist') {
                        const participantIds = new Set((meta?.participants || []).map(p => p.id));
                        const botNumLD = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        const hace7dias = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
                        const desconfis = (await User.find({ groupId: communityId, desconfi: true, confi: { $ne: true }, userId: { $ne: null } }))
                            .filter(u => {
                                if (!u.userId?.includes('@')) return false;
                                if (!participantIds.has(u.userId)) return false;
                                if (numFromJid(u.userId) === botNumLD) return false;
                                // Excluir nuevos (joinDate en los últimos 7 días) — solo mostrar "veteranos" desconfi
                                if (u.joinDate && new Date(u.joinDate) > hace7dias) return false;
                                return true;
                            });
                        if (!desconfis.length) { await reply('✅ No hay veteranos marcados con riesgo (flag) en el grupo.'); continue; }
                        let txt = '⤷ ゛🚩  ˎˊ˗\n♯ ·  · Riesgo (' + desconfis.length + ')  ่  ·  ▧\n\n';
                        desconfis.forEach(u => {
                            const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                            const desde = u.desconfiSince ? ` _(desde ${new Date(u.desconfiSince).toLocaleDateString('es')})_` : '';
                            txt += `⚠️ ${label}${desde}\n`;
                        });
                        await reply(txt, desconfis.map(u => u.userId)); continue;
                    }

                    // !adv
                    if (body.startsWith('!adv') && !body.startsWith('!desadv') && !body.startsWith('!veradv')) {
                        const advTarget = await resolve(communityId, rawText, target, reply);
                        if (!advTarget) { await reply('⚠️ Menciona a alguien o escribe su personaje.'); continue; }
                        if (await isGroupOwner(meta, advTarget)) { await react(message,'❎'); await reply('🚫 Los owners son inmunes a las advertencias.'); continue; }
                        const advPerso = (await User.findOne({groupId: communityId, userId: advTarget}))?.personaje || null;
                        const motivo = rawText.replace(/!adv/i,'').replace(/@\d+/g,'').trim().replace(advPerso ? new RegExp(advPerso,'i') : /x^/, '').trim() || 'Sin motivo';
                        const existing = await User.findOne({ groupId: communityId, userId: advTarget });
                        if ((existing?.advs || 0) >= MAX_ADV) {
                            await react(message,'⚠️');
                            await reply(`ℹ️ Ya tiene ${MAX_ADV}/${MAX_ADV}. Usa *!reset* primero.`, [advTarget]);
                            continue;
                        }
                        const u = await User.findOneAndUpdate(
                            { groupId: communityId, userId: advTarget },
                            { $inc: { advs: 1 }, $push: {
                                warnLog:  { motivo, fecha: new Date(), by: authorId },
                                staffLog: { accion: `Adv: ${motivo}`, fecha: new Date(), by: authorId }
                            }},
                            { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
                        );
                        await react(message, '✅');
                        await logAction(groupId, `Adv: ${motivo}`, authorId, advTarget);
                        await notificarAdv(groupId, u, motivo, authorId, meta, advTarget);
                        continue;
                    }

                    // !desadv / !reset — quitar UNA adv o resetear todas
                    if (body.startsWith('!desadv') || (body.startsWith('!reset') && body.includes('adv'))) {
                        const isReset = body.startsWith('!reset');
                        const desadvTarget = await resolve(communityId, rawText, target, reply);
                        if (!desadvTarget) { await reply('⚠️ Menciona a alguien o escribe su personaje.'); continue; }
                        const existing = await User.findOne({ groupId: communityId, userId: desadvTarget });
                        if (!existing || existing.advs <= 0) { await reply(`ℹ️ No tiene advs.`, [desadvTarget]); continue; }
                        const u = isReset
                            ? await User.findOneAndUpdate({ groupId: communityId, userId: desadvTarget },
                                { advs: 0, warnLog: [], $push: { staffLog: { accion: 'Advs reseteadas', fecha: new Date(), by: authorId } } },
                                { returnDocument: 'after' })
                            : await User.findOneAndUpdate({ groupId: communityId, userId: desadvTarget },
                                { $inc: { advs: -1 }, $pop: { warnLog: 1 }, $push: { staffLog: { accion: 'Adv removida', fecha: new Date(), by: authorId } } },
                                { returnDocument: 'after' });
                        const tLabel = u?.personaje ? `*${u.personaje}*` : `@${numFromJid(desadvTarget)}`;
                        await react(message, '✅');
                        await reply(isReset
                            ? `♻️ Advs de ${tLabel} reseteadas.`
                            : `✅ Adv quitada a ${tLabel}. Ahora: *${Math.max(0,u?.advs||0)}/${MAX_ADV}*`,
                            [desadvTarget]);
                        continue;
                    }

                    if (body === '!veradv') {
                        const users = await User.find({ groupId: communityId, advs: { $gt: 0 } });
                        if (!users.length) { await reply('✅ Sin advs activas.'); continue; }
                        let txt = '⤷ ゛⚠️  ˎˊ˗\n♯ ·  · Advertencias  ่  ·  ▧\n\n';
                        users.forEach(u => {
                            const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                            txt += `— ${label} *${u.advs}/${MAX_ADV}*\n`;
                            // Mostrar todas las advertencias (u.advs puede ser hasta MAX_ADV)
                            (u.warnLog || []).slice(-MAX_ADV).forEach((w,i) => txt += `   ${i+1}. _${w.motivo}_\n`);
                        });
                        await reply(txt, users.map(u => u.userId)); continue;
                    }

                    if (body === '!inactivos') {
                        const ahora = Date.now();
                        const hace3dias = new Date(ahora - 3 * 24 * 60 * 60 * 1000);
                        const hace7dias = new Date(ahora - 7 * 24 * 60 * 60 * 1000);
                        const participantIds = new Set((meta?.participants || []).map(p => p.id));
                        const botNumI = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                        // Solo usuarios que SÍ están en el grupo y tienen personaje
                        const allDbUsers = (await User.find({ groupId: communityId, personaje: { $ne: null }, userId: { $ne: null } }))
                            .filter(u => u.userId?.includes('@') && participantIds.has(u.userId) && numFromJid(u.userId) !== botNumI);
                        const users = allDbUsers.filter(u => {
                            if (u.excusa && u.excusaExpira && new Date(u.excusaExpira) > new Date()) return false; // con excusa vigente → no inactivo
                            return !u.lastSeen || new Date(u.lastSeen) < hace3dias;
                        });
                        if (!users.length) { await reply('✅ Sin inactivos (todos escribieron en los últimos 3 días).'); continue; }
                        users.sort((a,b) => (a.lastSeen ? new Date(a.lastSeen) : 0) - (b.lastSeen ? new Date(b.lastSeen) : 0));
                        let txt = '⤷ ゛⚰️  ˎˊ˗\n♯ ·  · Inactivos +3 días  ่  ·  ▧\n\n';
                        users.forEach(u => {
                            let tags = [];
                            if (!u.lastSeen) {
                                tags.push('nunca escribió');
                            } else {
                                const dias = Math.floor((ahora - new Date(u.lastSeen)) / 86400000);
                                tags.push(`${dias}d sin escribir`);
                            }
                            if (u.joinDate && new Date(u.joinDate) > hace7dias) tags.push('▫️ nuevo');
                            if (u.excusa) tags.push(`📌 ${u.excusa}`);
                            txt += `— *${u.personaje}* _(${tags.join(' · ')})_\n`;
                        });
                        await reply(txt); continue;
                    }

                    // !stats eliminado — usar !infogrupo (general)

                    // !limpiar — nota: WhatsApp solo permite borrar mensajes propios del bot
                    if (body === '!limpiar') {
                        await reply('ℹ️ WhatsApp no permite al bot borrar mensajes de otros usuarios. Usa la opción de limpiar chat manualmente.'); continue;
                    }

                    // !reservar — reserva un personaje por 3 semanas
                    if (body.startsWith('!reservar ')) {
                        const nomRes = rawText.slice(10).trim();
                        if (!nomRes) { await reply('⚠️ Uso: !reservar [nombre]'); continue; }
                        const yaOcupado = await User.findOne({ groupId: communityId, personaje: new RegExp(`^${nomRes}$`,'i') });
                        if (yaOcupado) { await reply(`🚫 *"${nomRes}"* ya está ocupado.`); continue; }
                        const yaRes = await Reservado.findOne({ groupId: communityId, personaje: new RegExp(`^${nomRes}$`,'i') });
                        if (yaRes) { await reply(`🔐 *"${nomRes}"* ya está reservado.`); continue; }
                        await Reservado.create({ groupId: communityId, personaje: nomRes, reservadoPor: authorId });
                        await react(message,'✅');
                        await reply(`🔐 *"${nomRes}"* reservado por 3 semanas.`);
                        continue;
                    }

                    // !desreservar — quitar reserva
                    if (body.startsWith('!desreservar ')) {
                        const nomDes = rawText.slice(13).trim();
                        if (!nomDes) { await reply('⚠️ Uso: !desreservar [nombre]'); continue; }
                        const res = await Reservado.findOneAndDelete({ groupId: communityId, personaje: new RegExp(`^${nomDes}$`,'i') });
                        if (!res) { await reply(`❌ *"${nomDes}"* no está reservado.`); continue; }
                        await react(message,'✅');
                        await reply(`✅ Reserva de *"${nomDes}"* eliminada.`);
                        continue;
                    }

                    // !quitardisponible — elimina un personaje específico de disponibles
                    if (body.startsWith('!quitardisponible ')) {
                        const nomQD = rawText.slice(18).trim();
                        if (!nomQD) { await reply('⚠️ Uso: !quitardisponible [nombre]'); continue; }
                        const delD = await Disponible.findOneAndDelete({ groupId: communityId, personaje: new RegExp(`^${nomQD}$`, 'i') });
                        if (!delD) { await reply(`❌ *"${nomQD}"* no está en disponibles.`); continue; }
                        await react(message, '✅');
                        await reply(`🗑️ *"${delD.personaje}"* eliminado de disponibles.`);
                        continue;
                    }

                    // !limpiardisp — borra TODOS los disponibles del grupo
                    if (body === '!limpiardisp') {
                        const cntD = await Disponible.countDocuments({ groupId: communityId });
                        if (!cntD) { await reply('ℹ️ La lista de disponibles ya está vacía.'); continue; }
                        await Disponible.deleteMany({ groupId: communityId });
                        await react(message, '✅');
                        await reply(`🗑️ *${cntD}* disponible(s) eliminados.`);
                        continue;
                    }

                    // !fixperso — corrige personajes duplicados en la DB
                    if (body === '!fixperso') {
                        await react(message, '⏳');
                        const users = await User.find({ groupId: communityId, personaje: { $ne: null } });
                        const seen = new Map();
                        let fixed = 0;
                        for (const u of users) {
                            const norm = normalizarNombre(u.personaje);
                            if (seen.has(norm)) {
                                const older = seen.get(norm);
                                const toReset = u.lastPersoChange > (older.lastPersoChange || 0) ? older : u;
                                await Disponible.create({ groupId: communityId, personaje: toReset.personaje });
                                await User.findOneAndUpdate({ groupId: communityId, userId: toReset.userId }, { personaje: null });
                                fixed++;
                            } else {
                                seen.set(norm, u);
                            }
                        }
                        await react(message, '✅');
                        await reply(fixed > 0 ? `🔧 ${fixed} personaje(s) duplicado(s) corregido(s).` : '✅ Sin duplicados encontrados.');
                        continue;
                    }

                    // !infogrupo — movido a general

                    // !eli — expulsa solo del grupo actual
                    if (body.startsWith('!eli') && !body.startsWith('!eliintercambio')) {
                        const cfgEli = await Config.findOne({ groupId });
                        if (cfgEli?.lockEliCmd && !isOW) { await react(message,'🔒'); await reply('🔒 !eli bloqueado.'); continue; }
                        if (!isOW && !userData?.confi) { await react(message,'🚫'); await reply('🚫 Necesitas *confi* para usar !eli.'); continue; }
                        const eliTarget = await resolve(communityId, rawText, target, reply);
                        if (!eliTarget) { await reply('⚠️ Menciona a alguien o escribe su personaje.'); continue; }
                        if (await isGroupOwner(meta, eliTarget)) { await react(message,'❎'); await reply('🚫 No puedes expulsar a un owner.'); continue; }
                        if (eliTarget === creator) { await react(message,'❎'); await reply('🚫 No puedes expulsar al creador.'); continue; }
                        try {
                            await sock.groupParticipantsUpdate(groupId, [eliTarget], 'remove');
                            await react(message, '✅');
                            await reply(`⤷ ゛☠️  ˎˊ˗\n♯ ·  · @${numFromJid(eliTarget)} fue mandado a la mrd.`, [eliTarget]);
                            await logAction(groupId, 'Expulsado !eli', authorId, eliTarget);
                        } catch (e) { await reply(`❌ ${e.message}`); }
                        continue;
                    }

                    // !ban — expulsa de TODOS los grupos de la comunidad
                    if (body.startsWith('!ban') && target) {
                        const cfgBan = await Config.findOne({ groupId });
                        if (cfgBan?.lockBan && !isOW) { await react(message,'🔒'); await reply('🔒 !ban bloqueado.'); continue; }
                        if (!isOW && !userData?.confi) { await react(message,'🚫'); await reply('🚫 Necesitas *confi* para usar !ban.'); continue; }
                        if (await isGroupOwner(meta, target)) { await react(message,'❎'); await reply('🚫 No puedes banear a un owner.'); continue; }
                        if (target === creator) { await react(message,'❎'); await reply('🚫 No puedes banear al creador.'); continue; }
                        try {
                            let grupos = [groupId];
                            if (meta?.linkedParent) {
                                try {
                                    const todos = await sock.groupFetchAllParticipating();
                                    grupos = [groupId, ...Object.values(todos)
                                        .filter(g => g.linkedParent === meta.linkedParent && g.id !== groupId)
                                        .map(g => g.id)];
                                } catch(_) {}
                            }
                            let n = 0;
                            for (const gid of grupos) {
                                markBotAction(gid, target, 'remove');
                                try { await sock.groupParticipantsUpdate(gid, [target], 'remove'); n++; } catch(_) {}
                            }
                            await react(message, '✅');
                            await reply(`⤷ ゛🔨  ˎˊ˗\n♯ ·  · @${numFromJid(target)} fue borrado del mapa en *${n} grupo(s)*. \n_Ni vuelvas._`, [target]);
                            await logAction(groupId, `Baneado !ban (${n} grupos)`, authorId, target);
                        } catch (e) { await reply(`❌ ${e.message}`); }
                        continue;
                    }

                    if (body.startsWith('!promover') && target) {
                        if (!isOW && !userData?.confi) { await react(message,'🚫'); await reply('🚫 Necesitas *confi* para usar !promover.'); continue; }
                        try {
                            markBotAction(groupId, target, 'promote'); try { await sock.groupParticipantsUpdate(groupId, [target], 'promote'); console.log('· promovido:', target); } catch(e) { console.error('· promover error:', e.message); }
                            await react(message,'✅'); await reply(`⤷ ゛⬆️  ˎˊ˗\n♯ ·  · @${numFromJid(target)} ahora tiene poder. Felicidades, nuevo esclavo.`,[target]);
                            await logAction(groupId,'Promovido',authorId,target);
                        } catch (e) { await reply(`❌ ${e.message}`); }
                        continue;
                    }

                    if (body.startsWith('!degradar') && target) {
                        if (!isOW && !userData?.confi) { await react(message,'🚫'); await reply('🚫 Necesitas *confi* para usar !degradar.'); continue; }
                        if (await isOwner(target)) { await react(message,'❎'); await reply('🚫 No puedes degradar a un owner.'); continue; }
                        if (target === creator) { await react(message,'❎'); await reply('🚫 No puedes degradar al creador.'); continue; }
                        try {
                            markBotAction(groupId, target, 'demote'); try { await sock.groupParticipantsUpdate(groupId, [target], 'demote'); console.log('· degradado:', target); } catch(e) { console.error('· degradar error:', e.message); }
                            await react(message,'✅'); await reply(`⤷ ゛⬇️  ˎˊ˗\n♯ ·  · @${numFromJid(target)} volvió a ser mortal. No sirves ni para irte a la mrd.`,[target]);
                            await logAction(groupId,'Degradado',authorId,target);
                        } catch (e) { await reply(`❌ ${e.message}`); }
                        continue;
                    }

                    if (body.startsWith('!nota') && target) {
                        const texto = rawText.replace(/!nota/i,'').replace(/@\d+/g,'').trim();
                        if (!texto) { await reply('⚠️ Uso: !nota @user texto'); continue; }
                        await User.findOneAndUpdate({ groupId: communityId, userId: target },
                            { $push: { notas: { texto, fecha: new Date(), by: authorId } } },
                            { upsert: true, setDefaultsOnInsert: true });
                        await react(message,'✅'); await reply(`📌 Nota para @${numFromJid(target)}.`,[target]); continue;
                    }

                    if (body.startsWith('!historial') && target) {
                        const u = await User.findOne({ groupId: communityId, userId: target });
                        if (!u || (!u.staffLog?.length && !u.notas?.length)) { await reply('Sin historial.'); continue; }
                        let txt = `⤷ ゛🗂️  ˎˊ˗\n♯ ·  · Historial — @${numFromJid(target)}  ่  ·  ▧\n\n`;
                        if (u.staffLog?.length) {
                            txt += `⚙️ *Acciones:*\n`;
                            u.staffLog.slice(-8).forEach(l => txt += `  — _${l.accion}_ · ${timeAgo(l.fecha)} · @${numFromJid(l.by)}\n`);
                        }
                        if (u.notas?.length) {
                            txt += `\n📌 *Notas:*\n`;
                            u.notas.slice(-5).forEach(n => txt += `  — _${n.texto}_ · ${timeAgo(n.fecha)} · @${numFromJid(n.by)}\n`);
                        }
                        await reply(txt, [target]); continue;
                    }

                    // !verexcusas — lista de excusas activas (staff)
                    if (body === '!verexcusas') {
                        const users = (await User.find({ groupId: communityId, excusa: { $ne: null }, userId: { $ne: null } })).filter(u => u.userId?.includes('@'));
                        if (!users.length) { await reply('Sin excusas activas.'); continue; }
                        let txt = '⤷ ゛📝  ˎˊ˗\n♯ ·  · Excusas Activas  ่  ·  ▧\n\n';
                        users.forEach(u => {
                            const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                            txt += `• ${label}: _${u.excusa}_\n`;
                        });
                        await reply(txt, users.map(u => u.userId)); continue;
                    }

                    // !parejas — movido a general
                }

                // ═══════════════════════════════════
                // GENERALES
                // ═══════════════════════════════════
                // !parejas — general, solo primeros nombres sin etiquetar
                if (body === '!parejas') {
                    const parejas = await Pareja.find({ groupId: communityId });
                    if (!parejas.length) { await reply('💑 Sin parejas registradas.'); continue; }
                    // Agrupar por persona para mostrar poliamor correctamente
                    const visto = new Set();
                    const lineas = [];
                    for (const p of parejas) {
                        const key = [p.user1,p.user2].sort().join('|');
                        if (visto.has(key)) continue;
                        visto.add(key);
                        const [u1,u2] = await Promise.all([
                            User.findOne({ groupId: communityId, userId: p.user1 }),
                            User.findOne({ groupId: communityId, userId: p.user2 })
                        ]);
                        const l1 = primerNombre(u1?.personaje) || '(sin personaje)';
                        const l2 = primerNombre(u2?.personaje) || '(sin personaje)';
                        lineas.push(`  ⊹ *${l1}* ʚ♡ɞ *${l2}*`);
                    }
                    let txt = '  ⤷ ゛💌  ˎˊ˗\n  ♯ ·  · Almas Gemelas  ่  ·  ▧\n\n' + lineas.join('\n');
                    await reply(txt); continue;
                }

                // !mipareja — muestra las parejas del usuario que lo envía
                if (body === '!mipareja' || body === '!mi pareja') {
                    const misParejas = await Pareja.find({ groupId: communityId, $or: [{ user1: authorId }, { user2: authorId }] });
                    if (!misParejas.length) { await reply('💔 No tienes pareja registrada.'); continue; }
                    const uMe = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNombre = primerNombre(uMe?.personaje || numFromJid(authorId));
                    const lineas = await Promise.all(misParejas.map(async p => {
                        const otherId = p.user1 === authorId ? p.user2 : p.user1;
                        const uOther = await User.findOne({ groupId: communityId, userId: otherId });
                        return `💕 *${primerNombre(uOther?.personaje || numFromJid(otherId))}*`;
                    }));
                    await reply(`  ⤷ ゛🌷  ˎˊ˗\n  ♯ ·  · El dueño de tu corazón  ่  ·  ▧\n\n  _Tu corazón le pertenece a:_\n${lineas.map(l => '  𐔌 . ' + l.replace(/💕 /,'')).join('\n')}`); continue;
                }

                // !infogrupo — general
                if (body === '!infogrupo' || body === '!grupo') {
                    const admins = getAdmins(meta);
                    const totalMiembros = meta?.participants?.length || 0;
                    const [conPerso, parejasC, conAdvs, reservados] = await Promise.all([
                        User.countDocuments({ groupId: communityId, personaje: { $ne: null } }),
                        Pareja.countDocuments({ groupId: communityId }),
                        User.countDocuments({ groupId: communityId, advs: { $gt: 0 } }),
                        Reservado.countDocuments({ groupId: communityId }),
                    ]);
                    let txt = `  ⤷ ゛🌍  ˎˊ˗\n  ♯ ·  · 𝓢tas del grupo  ่  ·  ▧\n\n`;
                    txt += `  📛 *Comunidad:* ${meta?.subject || 'Desconocido'}\n`;
                    txt += `  👥 *Población:* ${totalMiembros} miembros\n  🛡️ *Jerarquía:* ${admins.length} admins\n`;
                    txt += `  🎭 *Ocupados:* ${conPerso}  💑 *Enamorados:* ${parejasC}\n`;
                    txt += `  ⚠️ *Fichados:* ${conAdvs}   🔐 *En espera:* ${reservados}\n\n`;
                    txt += `  ⚙️ 𝓒onfiguración:\n`;
                    txt += `  ⊹ Antilink: ${cfg.antilink?'✅':'❌'}${cfg.lockAntilink?' 🔒':''}  ⊹ Autoclose: ${cfg.autoclose?'✅':'❌'}${cfg.lockAutoclose?' 🔒':''}\n`;
                    txt += `  ⊹ Antiporno: ${cfg.antiporno?'✅':'❌'}${cfg.lockAntiporno?' 🔒':''}  ⊹ Antiflood: ${cfg.antiflood?'✅':'❌'}${cfg.lockAntiflood?' 🔒':''}\n`;
                    txt += `  ⊹ Autoban: 𝚐𝚘𝚛𝚎 ${cfg.autobanGore?'✅':'❌'}  𝚗𝚜𝚏𝚠 ${cfg.autobanNsfw?'✅':'❌'}  𝚊𝚍𝚟 ${cfg.autobanAdv?'✅':'❌'}\n`;
                    await react(message, '✅');
                    await reply(txt); continue;
                }

                if (body === '!menu') { await react(message, '✅'); await reply(MENU_GENERAL); continue; }
                if (body === '!acciones') { await react(message, '✅'); await reply(MENU_ACCIONES); continue; }

                // ── !besar / !kiss — mandar beso con GIF aleatorio ──
                if (body.startsWith('!besar') || body.startsWith('!kiss')) {
                    const targetBesar = target || getMentions(message)[0];
                    const uYoBesar = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNBesar = nombreDisplay(uYoBesar) || `@${numFromJid(authorId)}`;
                    const gifUrl = await gifAleatorio('kiss');
                    const caption = targetBesar
                        ? `  ┆ ⤿ 💋 ⌗ 𐔌 .

  *${miNBesar}* le da un beso a *${nombreDisplay(await User.findOne({ groupId: communityId, userId: targetBesar })) || `@${numFromJid(targetBesar)}`}* 💋`
                        : `  ┆ ⤿ 💋 ⌗ 𐔌 .

  *${miNBesar}* lanza un beso al aire... ¿Alguien lo atrapa? 💋`;
                    const gifBuf = await downloadGif(gifUrl);
                    if (gifBuf) {
                        await sock.sendMessage(groupId, {
                            video: gifBuf,
                            gifPlayback: true,
                            caption,
                            mentions: targetBesar ? [targetBesar] : []
                        });
                    } else {
                        await sock.sendMessage(groupId, { text: caption, mentions: targetBesar ? [targetBesar] : [] });
                    }
                    continue;
                }

                // ── !hug — mandar abrazo con GIF aleatorio ──
                if (body.startsWith('!hug')) {
                    const targetHug = target || getMentions(message)[0];
                    const uYoHug = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNHug = nombreDisplay(uYoHug) || `@${numFromJid(authorId)}`;
                    const gifUrl = await gifAleatorio('hug');
                    const captionHug = targetHug
                        ? `  ┆ ⤿ 🤗 ⌗ 𐔌 .

  *${miNHug}* abraza fuerte a *${nombreDisplay(await User.findOne({ groupId: communityId, userId: targetHug })) || `@${numFromJid(targetHug)}`}* 🤗`
                        : `  ┆ ⤿ 🤗 ⌗ 𐔌 .

  *${miNHug}* abre los brazos esperando un abrazo... 🤗`;
                    const gifBuf = await downloadGif(gifUrl);
                    if (gifBuf) {
                        await sock.sendMessage(groupId, {
                            video: gifBuf,
                            gifPlayback: true,
                            caption: captionHug,
                            mentions: targetHug ? [targetHug] : []
                        });
                    } else {
                        await sock.sendMessage(groupId, { text: captionHug, mentions: targetHug ? [targetHug] : [] });
                    }
                    continue;
                }

                // ── !v2a — convertir video a audio (nota de voz) ──
                // FIX C-2: cleanup garantizado en finally (vía videoToAudio de media.js)
                // FIX A-6: mimetype corregido a 'audio/ogg; codecs=opus' para PTT real en iOS/Android
                if (body === '!v2a') {
                    const quotedV = message.message?.extendedTextMessage?.contextInfo?.quotedMessage;
                    if (!quotedV?.videoMessage) { await reply('⚠️ Responde a un video con *!v2a*'); continue; }
                    await react(message, '⏳');
                    try {
                        const fakeVideoMsg = { key: message.key, message: quotedV };
                        const videoBuffer = await downloadVideo(fakeVideoMsg);
                        if (!videoBuffer) { await reply('❌ No pude descargar el video.'); continue; }
                        // videoToAudio maneja tmpIn/tmpOut en finally — nunca deja basura en /tmp
                        const result = await videoToAudio(videoBuffer, true); // true = ogg/opus para PTT
                        if (!result) { await reply('❌ Error al convertir el video.'); continue; }
                        await sock.sendMessage(groupId, {
                            audio: result.buffer,
                            mimetype: result.mimetype, // 'audio/ogg; codecs=opus'
                            ptt: true
                        }, { quoted: message });
                        await react(message, '✅');
                    } catch(e) {
                        console.error('· v2a error:', e.message);
                        await reply('❌ Error al convertir el video.');
                    }
                    continue;
                }

                // ── !s — convertir imagen/GIF a sticker ──
                if (body === '!s') {
                    const quotedS = message.message?.extendedTextMessage?.contextInfo?.quotedMessage;

                    // Detectar si es imagen estática o GIF animado (videoMessage con gifPlayback)
                    const isDirectImg = !!message.message?.imageMessage;
                    const isDirectGif = !!message.message?.videoMessage?.gifPlayback;
                    const isQuotedImg = !!quotedS?.imageMessage;
                    const isQuotedGif = !!quotedS?.videoMessage?.gifPlayback;

                    const isMedia = isDirectImg || isDirectGif || isQuotedImg || isQuotedGif;
                    if (!isMedia) { await reply('⚠️ Responde a una imagen o GIF con *!s*, o envía la imagen/GIF con *!s* como caption.'); continue; }

                    await react(message, '⏳');
                    try {
                        let mediaBuffer = null;

                        if (isQuotedImg) {
                            // Imagen citada
                            const fakeMsg = { key: message.key, message: quotedS };
                            mediaBuffer = await downloadMedia(fakeMsg, 5 * 1024 * 1024);
                        } else if (isQuotedGif) {
                            // GIF citado — descargarlo como video
                            const fakeMsg = { key: message.key, message: quotedS };
                            mediaBuffer = await downloadVideo(fakeMsg);
                        } else if (isDirectImg) {
                            // Imagen enviada directamente con caption !s
                            mediaBuffer = await downloadMedia(message, 5 * 1024 * 1024);
                        } else if (isDirectGif) {
                            // GIF enviado directamente con caption !s
                            mediaBuffer = await downloadVideo(message);
                        }

                        if (!mediaBuffer) { await reply('❌ No pude descargar el archivo.'); continue; }

                        // FIX M-6: stickerType siempre fue StickerTypes.FULL — condición ternaria era dead code
                        const sticker = new Sticker(mediaBuffer, {
                            pack: 'Beyonder',
                            author: 'Beyonder',
                            type: StickerTypes.FULL,
                            quality: 70,
                        });
                        const stickerBuffer = await sticker.toBuffer();
                        await sock.sendMessage(groupId, { sticker: stickerBuffer }, { quoted: message });
                        await react(message, '✅');
                    } catch(e) {
                        console.error('· sticker error:', e.message);
                        await reply('❌ Error al crear el sticker.');
                    }
                    continue;
                }

                // ── !ia — hablar con el cerebro Phi-3 directamente ──
                if (body.startsWith('!ia')) {
                    const preguntaIA = rawText.replace(/^!ia\s*/i, '').trim();
                    if (!preguntaIA) { await reply('¿qué me decís? ej: _!ia qué onda_'); continue; }

                    const iaFatigue = trackCommandFatigue(groupId, 'ia');
                    if (iaFatigue.nivel === 3) { await reply(getFatigeResponse(3)); continue; }

                    const ctx    = await getContextoIA(communityId, authorId, meta, cfg.groupPersonality);
                    await simulateTyping(sock, groupId);
                    const resp   = await obtenerRespuestaIA(preguntaIA, ctx.nombreUsuario, ctx);
                    await humanDelay(sock, groupId);

                    if (!resp) { await reply('me dio un mareo, vuelvo ahora'); continue; }

                    _lastUserBotChat.set(`${communityId}_${authorId}`, Date.now());
                    BeyondMemory.findOneAndUpdate({ communityId, userId: authorId }, { lastOrganic: new Date() }, { upsert: true }).catch(() => {});
                    updateResumenPersonalidad(communityId, authorId, preguntaIA, resp).catch(() => {});
                    await reply(resp);
                    continue;
                }

                // ── GUARDAR MENSAJE EN CONTEXTO DE GRUPO (MongoDB) ──
                if (!body.startsWith('!') && rawText.trim().length >= 1) {
                    const senderName = message.pushName || numFromJid(authorId);
                    saveGroupMessage(groupId, authorId, senderName, rawText).catch(() => {});
                }

                // ── INTERACCIÓN ORGÁNICA — responde si lo mencionan, le responden o hay conversación activa ──
                if (!body.startsWith('!') && rawText.trim().length >= 1) {
                    const mentionedJids = message.message?.extendedTextMessage?.contextInfo?.mentionedJid || [];
                    const isMentioned   = BOT_NUM && mentionedJids.some(j => numFromJid(j) === BOT_NUM);
                    const mentionBot    = /beyond(er)?/i.test(rawText);
                    const isReplyBot    = !!quotedParticipant && numFromJid(quotedParticipant) === BOT_NUM;
                    const CONVO_TTL_MS  = 5 * 60 * 1000;
                    const lastChat      = _lastUserBotChat.get(`${communityId}_${authorId}`) || 0;
                    const isActiveConvo = (Date.now() - lastChat) < CONVO_TTL_MS;

                    const shouldRespond = isMentioned || mentionBot || isReplyBot || isActiveConvo;
                    if (shouldRespond) {
                        // Debounce: acumula mensajes en ráfaga antes de responder
                        const debounceKey = `${communityId}_${authorId}`;
                        const existing    = _msgDebounce.get(debounceKey);
                        const parts       = existing ? existing.parts : [];
                        parts.push(rawText.trim());
                        const accMention   = (existing?.mentionBot  || false) || mentionBot;
                        const accReply     = (existing?.isReplyBot  || false) || isReplyBot;
                        const accMentioned = (existing?.isMentioned || false) || isMentioned;
                        if (existing?.timer) clearTimeout(existing.timer);

                        const timer = setTimeout(async () => {
                            _msgDebounce.delete(debounceKey);
                            const fullText = parts.join('\n');

                            // Cooldown de 8s para conversación activa (menciones siempre pasan)
                            const orgCoolKey = `${communityId}_${authorId}_org`;
                            const lastOrg    = _organicCooldown.get(orgCoolKey) || 0;
                            if (!(accMentioned || accReply) && (Date.now() - lastOrg < 8000)) return;
                            _organicCooldown.set(orgCoolKey, Date.now());

                            try {
                                if (fullText.split(/\s+/).length > 100) { await reply('mucho texto, resumelo'); return; }

                                // Obtener contexto de MongoDB y pedir respuesta al cerebro
                                const ctx  = await getContextoIA(communityId, authorId, meta, cfg.groupPersonality);
                                await simulateTyping(sock, groupId);
                                const resp = await obtenerRespuestaIA(fullText, ctx.nombreUsuario, ctx);
                                if (!resp || !resp.trim()) return;

                                await humanDelay(sock, groupId);
                                _lastUserBotChat.set(`${communityId}_${authorId}`, Date.now());
                                _lastBotMsgTime.set(communityId, Date.now());
                                if (accMention) {
                                    if (esInsultoAlBot(fullText))    updateSentimiento(communityId, authorId, -1).catch(() => {});
                                    else if (esTratoBueno(fullText)) updateSentimiento(communityId, authorId, +1).catch(() => {});
                                }
                                BeyondMemory.findOneAndUpdate({ communityId, userId: authorId }, { lastOrganic: new Date() }, { upsert: true }).catch(() => {});
                                updateResumenPersonalidad(communityId, authorId, fullText, resp).catch(() => {});
                                await reply(resp);
                            } catch(e) { console.error('· orgánica error:', e.message); }
                        }, DEBOUNCE_MS);

                        _msgDebounce.set(debounceKey, { timer, parts, mentionBot: accMention, isReplyBot: accReply, isMentioned: accMentioned });
                    }
                }

                // Bloquear !pmenu y comandos owner a no-owners
                const OWNER_CMDS = ['!pmenu','!ownerping','!nuke','!logs','!lock','!beyonder','!vip','!novip','!flag','!delete data','!delete grupo','!delete comunidad','!sí','!si','!no'];
                if (!isOW && OWNER_CMDS.some(c => body.startsWith(c))) {
                    await react(message, '🚫'); continue;
                }

                // Bloquear comandos de staff a no-admins
                const STAFF_CMDS = ['!smenu','!ping','!adv','!desadv','!veradv','!reset','!autoban','!reservar','!desreservar','!aceptar cambio','!negar cambio',
                    '!inactivos','!lista','!viplist','!risklist','!sinperso','!top10','!activos','!asignarperso','!redesc','!desname',
                    '!cambiarperso','!resetperso','!moverperso','!nota','!historial',
                    '!eli','!ban','!promover','!degradar','!abrir','!cerrar','!setreglas',
                    '!antilink','!autoclose','!verexcusas','!fixperso','!limpiar',
                    '!quitardisponible','!limpiardisp'];
                if (!isAdmin && !isOW && STAFF_CMDS.some(c => body.startsWith(c))) {
                    await react(message, '❌'); continue;
                }

                // !lista2 — disponible para todos (antes !personajes)
                if (body === '!lista2' || body === '!personajes') {
                    const [ocupados, buscados, disponibles, reservados] = await Promise.all([
                        User.find({ groupId: communityId, personaje: { $ne: null } }),
                        PersoBuscado.find({ groupId: communityId }),
                        Disponible.find({ groupId: communityId }),
                        Reservado.find({ groupId: communityId }),
                    ]);
                    const fmtOcup = u => {
                        const alias = nombreDisplay(u);
                        const showAlias = alias !== u.personaje ? ` _(${alias})_` : '';
                        return `— *${u.personaje}*${showAlias}${u.excusa ? ' 📌' : ''}\n`;
                    };
                    let ptxt = '⤷ ゛📋  ˎˊ˗\n♯ ·  · Personajes  ่  ·  ▧\n\n';
                    if (ocupados.length) { ptxt += `🎭 *Ocupados (${ocupados.length}):*\n`; ocupados.forEach(u => ptxt += fmtOcup(u)); }
                    if (reservados.length) { ptxt += `\n🔐 *Reservados (${reservados.length}):*\n`; reservados.forEach(r => ptxt += `— *${r.personaje}*\n`); }
                    if (buscados.length)   { ptxt += `\n🔎 *Buscados (${buscados.length}):*\n`;   buscados.forEach(b => ptxt += `— *${b.personaje}*\n`); }
                    if (disponibles.length){ ptxt += `\n🕊️ *Disponibles (${disponibles.length}):*\n`; disponibles.forEach(d => ptxt += `— *${d.personaje}*\n`); }
                    await reply(ptxt); continue;
                }

                // !reporte
                if (body.startsWith('!reporte')) {
                    const motivo = rawText.slice(8).trim() || 'sin motivo';
                    // Filtrar: no etiquetar al autor, ni al bot, ni a sí mismo
                    const admins = getAdmins(meta).filter(a =>
                        numFromJid(a) !== numFromJid(authorId) &&
                        numFromJid(a) !== botNum
                    );
                    if (!admins.length) { await reply('ℹ️ No hay admins disponibles.'); continue; }
                    let txt = `🚨 *REPORTE*\n\n👤 *De:* @${numFromJid(authorId)}\n📋 *Motivo:* _${motivo}_\n\n`;
                    txt += admins.map(a => `@${numFromJid(a)}`).join(' ');
                    await react(message, '✅');
                    await reply(txt, [authorId, ...admins]); continue;
                }

                // !sugerencia — usuario envía sugerencia a admins y la guarda en DB
                if (body.startsWith('!sugerencia')) {
                    const sugerencia = rawText.slice(11).trim();
                    if (!sugerencia) { await reply('❌ Escribe tu sugerencia. Ej: _!sugerencia más canales_'); continue; }
                    const adminsS = getAdmins(meta).filter(a => a !== authorId);
                    const uS = await User.findOne({ groupId: communityId, userId: authorId });
                    const sLabel = uS?.personaje ? `*${uS.personaje}*` : `@${numFromJid(authorId)}`;
                    // Guardar en DB
                    await Sugerencia.create({ groupId: communityId, userId: authorId, texto: sugerencia });
                    if (adminsS.length) {
                        await sendText(groupId,
                            `💌 *Sugerencia de ${sLabel}:*\n_${sugerencia}_\n\n` +
                            adminsS.slice(0,3).map(a => `@${numFromJid(a)}`).join(' '),
                            [authorId, ...adminsS.slice(0,3)]);
                    }
                    await react(message, '✅');
                    await reply(`   ⤷ ゛💌  ˎˊ˗\n  ♯ ·  · Sugerencia enviada a los admins.\n  _Gracias por intentar que este lugar no sea un caos._`);
                    continue;
                }

                // !versuge — ver sugerencias guardadas (solo admins/owners)
                if (body === '!versuge' || body.startsWith('!versuge ')) {
                    if (!isAdmin && !isOW) { await react(message, '🚫'); continue; }
                    const limit = parseInt(body.split(' ')[1]) || 20;
                    const suges = await Sugerencia.find({ groupId: communityId }).sort({ fecha: -1 }).limit(limit);
                    if (!suges.length) { await reply('💌 No hay sugerencias guardadas.'); continue; }
                    let stxt = `⤷ ゛💌  ˎˊ˗\n♯ ·  · Sugerencias (${suges.length})  ่  ·  ▧\n\n`;
                    for (const s of suges) {
                        const uSuge = await User.findOne({ groupId: communityId, userId: s.userId });
                        const lbl = uSuge?.personaje ? uSuge.personaje : numFromJid(s.userId);
                        const fecha = new Date(s.fecha).toLocaleDateString('es', { day:'2-digit', month:'2-digit' });
                        stxt += `💬 *${lbl}* _(${fecha})_:\n_${s.texto}_\n\n`;
                    }
                    await reply(stxt.trim()); continue;
                }

                // !perso [nombre] — SOLO admins/owners asignan directamente
                if (body.startsWith('!perso ') && !body.startsWith('!persoinfo')) {
                    if (!isAdmin && !isOW) { await reply('⚠️ Usa *!cambio perso [nombre]* para solicitar un personaje.'); continue; }
                    const p = rawText.slice(7).trim().replace(/@\d+/g,'').trim();
                    if (!p) { await reply('❌ Escribe el nombre del personaje.'); continue; }
                    if (cfg.lockperso && !isOW) { await react(message,'🚫'); await reply('🔒 Los personajes están bloqueados.'); continue; }
                    const persoTarget = target || authorId;
                    const allUsers = await User.find({ groupId: communityId, personaje: { $ne: null } });
                    const normP = normalizarNombre(p);
                    const dup = allUsers.find(u => u.userId !== persoTarget && normalizarNombre(u.personaje || '') === normP);
                    if (dup) { await reply(`🚫 *"${p}"* ya lo tiene *${dup.personaje}*.`, [dup.userId]); continue; }
                    const uT = await User.findOne({ groupId: communityId, userId: persoTarget });
                    const ant = uT?.personaje || null;
                    if (ant) await Disponible.create({ groupId: communityId, personaje: ant }).catch(()=>{});
                    await User.findOneAndUpdate({ groupId: communityId, userId: persoTarget },
                        { personaje: p, lastPersoChange: new Date(), $push: { staffLog: { accion: `Perso asignado: ${p}`, fecha: new Date(), by: authorId } } },
                        { upsert: true, setDefaultsOnInsert: true });
                    await Disponible.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    await PersoBuscado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    await Reservado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    limpiarDupDisponibles(communityId, p).catch(() => {});
                    await react(message, '✅');
                    let r = `🎭 *"${p}"* asignado a @${numFromJid(persoTarget)}.`;
                    if (ant) r += `
🕊️ *"${ant}"* queda disponible.`;
                    await reply(r, [persoTarget]);
                    await logAction(groupId, `Perso asignado: ${p}`, authorId, persoTarget);
                    continue;
                }

                // !cambio perso [nombre] — usuarios solicitan cambio, admin aprueba/niega
                if (body.startsWith('!cambio perso ')) {
                    if (isAdmin || isOW) { await reply('⚠️ Admins usan *!perso @user [nombre]* directamente.'); continue; }
                    const p = rawText.slice(14).trim();
                    if (!p) { await reply('❌ Escribe el nombre: !cambio perso [nombre]'); continue; }
                    if (cfg.lockperso) { await react(message,'🚫'); await reply('🔒 Los personajes están bloqueados.'); continue; }
                    const resCheck = await Reservado.findOne({ groupId: communityId, personaje: new RegExp(`^${p}$`,'i') });
                    if (resCheck) { await reply(`🔐 *"${p}"* está reservado por el staff.`); continue; }
                    const allUsers2 = await User.find({ groupId: communityId, personaje: { $ne: null } });
                    const normP2 = normalizarNombre(p);
                    const dup2 = allUsers2.find(u => u.userId !== authorId && normalizarNombre(u.personaje || '') === normP2);
                    if (dup2) { await reply(`🚫 *"${p}"* ya está ocupado.`); continue; }
                    // Guardar solicitud pendiente en memoria (Map global)
                    
                    solicitudesCambioPerso.set(authorId, { groupId: communityId, personaje: p, fecha: Date.now() });
                    // Limpiar solicitud después de 10 min
                    setTimeout(() => { if (solicitudesCambioPerso?.get(authorId)?.personaje === p) solicitudesCambioPerso.delete(authorId); }, 10 * 60 * 1000);
                    const allAdmins2 = getAdmins(meta).filter(a => a !== authorId);
                    const notifyAdmins2 = allAdmins2.sort(() => Math.random() - 0.5).slice(0, 2);
                    await react(message, '⏳');
                    await reply(
                        `╭━━━━━━━━━━━━━━━━━━━━━━━\n  📝 *Solicitud de personaje*\n  @${numFromJid(authorId)} quiere *"${p}"*.\n\n  ✅ *!aceptar cambio @${numFromJid(authorId)}*\n  ❌ *!negar cambio @${numFromJid(authorId)}*\n\n  ${notifyAdmins2.map(a => `@${numFromJid(a)}`).join(' ')}\n╰━━━━━━━━━━━━━━━━━━━━━━━`,
                        [authorId, ...notifyAdmins2]);
                    continue;
                }

                if (body.startsWith('!buscarperso ')) {
                    const n = rawText.slice(13).trim();
                    const u = await User.findOne({ groupId: communityId, personaje: new RegExp(n,'i') });
                    if (!u) { await reply('❌ Nadie tiene ese personaje.'); continue; }
                    await reply(`🔍 *"${u.personaje}"* → @${numFromJid(u.userId)}`, [u.userId]); continue;
                }

                if (body.startsWith('!persoinfo ')) {
                    const n = rawText.slice(11).trim();
                    // Buscar por nombre normalizado
                    const allP = await User.find({ groupId: communityId, personaje: { $ne: null } });
                    const normN = normalizarNombre(n);
                    const u = allP.find(x => normalizarNombre(x.personaje||'') === normN) || allP.find(x => x.personaje?.toLowerCase().includes(n.toLowerCase()));
                    if (!u) { await reply('❌ Personaje no encontrado.'); continue; }
                    let txt = `🎭 *FICHA*\n\n📛 *Nombre:* ${u.personaje}\n👤 @${numFromJid(u.userId)}\n📩 *Mensajes:* ${u.msgCount}\n📌 *Estado:* ${u.excusa?'Inactivo':'Activo'}\n`;
                    await reply(txt, [u.userId]); continue;
                }

                // !disponibles — solo muestra los que realmente no tienen dueño activo
                if (body === '!disponibles' || body === '!ocupados') {
                    if (body === '!ocupados') {
                        // Lista de personajes ocupados SIN etiquetar
                        const ocupados = await User.find({ groupId: communityId, personaje: { $ne: null }, userId: { $ne: null } });
                        const reservados = await Reservado.find({ groupId: communityId });
                        const buscados = await PersoBuscado.find({ groupId: communityId });
                        let txt = '⤷ ゛📋  ˎˊ˗\n♯ ·  · Personajes Ocupados  ่  ·  ▧\n\n';
                        if (ocupados.length) {
                            txt += '🎭 *Ocupados:*\n';
                            ocupados.forEach(u => txt += `— *${u.personaje}*\n`);
                        } else { txt += '_Sin personajes ocupados_\n'; }
                        if (reservados.length) {
                            txt += '\n🔐 *Reservados:*\n';
                            reservados.forEach(r => txt += `— *${r.personaje}* _(caduca en ${Math.max(0,Math.ceil((new Date(r.fecha).getTime()+21*24*60*60*1000-Date.now())/(1000*60*60*24)))}d)_\n`);
                        }
                        if (buscados.length) {
                            txt += '\n🔎 *Buscados:*\n';
                            buscados.forEach(b => txt += `— *${b.personaje}*\n`);
                        }
                        await reply(txt); continue;
                    }
                    // !disponibles — igual que antes
                    if (body === '!disponibles') {
                    const ocupados = await User.find({ groupId: communityId, personaje: { $ne: null } }).select('personaje');
                    const normsOcupados = ocupados.map(u => normalizarNombre(u.personaje || '')).filter(Boolean);
                    const lista = (await Disponible.find({ groupId }).sort({ fecha: -1 }))
                        .filter(d => !normsOcupados.includes(normalizarNombre(d.personaje || '')));
                    if (!lista.length) { await reply('No hay personajes disponibles.'); continue; }
                    let txt = '⤷ ゛🕊️  ˎˊ˗\n♯ ·  · Disponibles  ่  ·  ▧\n\n';
                    lista.forEach(d => txt += `— *${d.personaje}* · hace ${timeAgo(d.fecha)}\n`);
                        await reply(txt); continue;
                    }
                }

                // !persobuscado — marcar personaje buscado
                if (body.startsWith('!persobuscado ')) {
                    const nomBus = rawText.slice(14).trim();
                    if (!nomBus) { await reply('⚠️ Uso: !persobuscado [nombre]'); continue; }
                    const yaOcup = await User.findOne({ groupId: communityId, personaje: new RegExp(`^${nomBus}$`,'i') });
                    if (yaOcup) { await reply(`🚫 *"${nomBus}"* ya está ocupado.`); continue; }
                    const yaB = await PersoBuscado.findOne({ groupId: communityId, personaje: new RegExp(`^${nomBus}$`,'i') });
                    if (yaB) { await reply(`🔎 *"${nomBus}"* ya está en la lista de buscados.`); continue; }
                    await PersoBuscado.create({ groupId: communityId, personaje: nomBus, buscadoPor: authorId });
                    await react(message,'✅');
                    await reply(`🔎 *"${nomBus}"* agregado a buscados.`);
                    continue;
                }

                // !buscados — lista de personajes buscados
                if (body === '!buscados') {
                    const buscados = await PersoBuscado.find({ groupId: communityId });
                    if (!buscados.length) { await reply('🔎 Sin personajes buscados.'); continue; }
                    let txt = '⤷ ゛🔎  ˎˊ˗\n♯ ·  · Personajes Buscados  ่  ·  ▧\n\n';
                    buscados.forEach(b => txt += `— *${b.personaje}*\n`);
                    await reply(txt); continue;
                }

                if (body === '!reglas') {
                    const c = await Config.findOne({ groupId });
                    if (!c?.reglas) { await reply('📜 Sin reglas. Un admin usa *!setreglas*.'); continue; }
                    await reply(`📜 *REGLAS*\n\n${c.reglas}`); continue;
                }

                // !info — por mención, por respuesta, por nombre de perso, o propio
                if (body.startsWith('!info')) {
                    const infoTarget = await resolveTarget(communityId, rawText, target) || authorId;
                    const u = await User.findOne({ groupId: communityId, userId: infoTarget });
                    if (!u && infoTarget !== authorId) { await reply('❌ Sin datos para ese usuario.'); continue; }
                    const isTargetOwner = await isOwner(infoTarget);
                    const isTargetAdmin = isAdm(meta, infoTarget);
                    let txt = `  ⤷ ゛🎐  ˎˊ˗\n  ♯ ·  · 𝓔xpediente: @${numFromJid(infoTarget)}  ่  ·  ▧\n\n`;
                    txt += '';
                    if (isTargetOwner)      txt += `  👑 *Rango:* Owner\n`;
                    else if (isTargetAdmin) txt += `  🛡️ *Rango:* Admin\n`;
                    txt += `  🎭 *Perso:* ${u?.personaje || 'Sin asignar'}\n`;
                    txt += `  ⚠️ *Récord:* ${u?.advs || 0}/${MAX_ADV} advertencias\n`;
                    if (u?.warnLog?.length) {
                        u.warnLog.slice(-3).forEach((w,i) => txt += `   ${i+1}. _${w.motivo}_\n`);
                    }
                    txt += `  📊 *Actividad:* ${u?.msgCount || 0} mensajes\n`;
                    // Estado
                    if (u?.banned)      txt += `  🔨 *Estado:* Baneado\n`;
                    else if (u?.silenciado) txt += `  🔇 *Estado:* Silenciado\n`;
                    else if (u?.excusa) txt += `  📌 *Estado:* Inactivo (_${u.excusa}_)\n`;
                    else               txt += `  📌 *Estado:* Activo\n`;
                    const pareja = await Pareja.findOne({ groupId: communityId, $or:[{user1:infoTarget},{user2:infoTarget}] });
                    if (pareja) {
                        const exId = pareja.user1 === infoTarget ? pareja.user2 : pareja.user1;
                        const exUser = await User.findOne({ groupId: communityId, userId: exId });
                        const exLabel = exUser?.personaje ? `*${exUser.personaje}*` : `@${numFromJid(exId)}`;
                        txt += `  💍 *Lazos:* ${exLabel}\n`;
                    }
                    await reply(txt, [infoTarget]); continue;
                }

                if (body === '!top10') {
                    const participantIds = new Set((meta?.participants || []).map(p => p.id));
                    const botNumT = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                    const users = (await User.find({ groupId: communityId, userId: { $ne: null } }).sort({ msgCount: -1 }))
                        .filter(u => u.userId?.includes('@') && participantIds.has(u.userId) && numFromJid(u.userId) !== botNumT)
                        .slice(0, 10);
                    if (!users.length) { await reply('Sin datos.'); continue; }
                    const medals = ['🥇','🥈','🥉'];
                    let txt = '⤷ ゛🏆  ˎˊ˗\n♯ ·  · Top 10 Activos  ่  ·  ▧\n\n';
                    users.forEach((u,i) => {
                        const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                        txt += `${medals[i]||`${i+1}.`} ${label} — *${u.msgCount} msgs*\n`;
                    });
                    await reply(txt, users.map(u => u.userId)); continue;
                }

                if (body === '!activos') {
                    const hoyStr = new Date().toISOString().slice(0,10);
                    const participantIds = new Set((meta?.participants || []).map(p => p.id));
                    const botNumA = BOT_NUM || (sock.user?.id || '').replace(/:[^@]+@/, '@').split('@')[0];
                    const users = (await User.find({ groupId: communityId, userId: { $ne: null }, dailyMsgDate: hoyStr }))
                        .filter(u => u.userId?.includes('@') && participantIds.has(u.userId) && numFromJid(u.userId) !== botNumA
                                     && (u.dailyMsgCount || 0) > 0)
                        .sort((a,b) => (b.dailyMsgCount||0) - (a.dailyMsgCount||0));
                    if (!users.length) { await reply('📭 Nadie ha escrito hoy todavía.'); continue; }
                    let txt = `⤷ ゛✍️  ˎˊ˗\n♯ ·  · Activos hoy (${users.length})  ่  ·  ▧\n\n`;
                    users.forEach((u,i) => {
                        const label = u.personaje ? `*${u.personaje}*` : `@${numFromJid(u.userId)}`;
                        txt += `${i+1}. ${label} — *${u.dailyMsgCount} msgs*\n`;
                    });
                    await reply(txt, users.map(u => u.userId)); continue;
                }

                // Excusas — !excusa [motivo] o !excusa [motivo] [Nd] (1-30 días, default 7)
                if (body.startsWith('!excusa ') && !body.startsWith('!excusa off')) {
                    const excusaTarget = (isAdmin || isOW) ? (await resolveTarget(communityId, rawText, target) || authorId) : authorId;
                    let motivoRaw = rawText.slice(8).replace(/@\d+/g, '').trim();
                    const persoExc = (await User.findOne({ groupId: communityId, userId: excusaTarget }))?.personaje || '';
                    if (persoExc) motivoRaw = motivoRaw.replace(new RegExp(persoExc, 'i'), '').trim();
                    // Detectar duración al final: ej. '14d'
                    let excusaDias = 7;
                    const durM = motivoRaw.match(/(\d+)d$/i);
                    if (durM) { excusaDias = Math.min(30, Math.max(1, parseInt(durM[1]))); motivoRaw = motivoRaw.slice(0, -durM[0].length).trim(); }
                    if (!motivoRaw) { await reply('❌ Escribe el motivo. Ej: _!excusa viaje_ o _!excusa viaje 14d_'); continue; }
                    const excusaExpira = new Date(Date.now() + excusaDias * 24 * 60 * 60 * 1000);
                    await User.findOneAndUpdate({ groupId: communityId, userId: excusaTarget },
                        { excusa: motivoRaw, excusaFecha: new Date(), excusaExpira },
                        { upsert: true, setDefaultsOnInsert: true });
                    const excLabel = excusaTarget === authorId ? 'Tu excusa' : `Excusa de @${numFromJid(excusaTarget)}`;
                    await react(message, '✅');
                    await reply(`✅ ${excLabel} guardada. _Caduca en ${excusaDias} día(s)._`, [excusaTarget]); continue;
                }
                if (body.startsWith('!excusa off')) {
                    const excusaOffTarget = (isAdmin || isOW) ? (await resolveTarget(communityId, rawText, target) || authorId) : authorId;
                    await User.findOneAndUpdate({ groupId: communityId, userId: excusaOffTarget }, { excusa: null, excusaFecha: null });
                    await react(message,'✅'); await reply(`  ⤷ ゛🗞️  ˎˊ˗\n  ♯ ·  · Excusa borrada.\n  _Ya no tienes excusa para estar de vago._`, [excusaOffTarget]); continue;
                }
                if (body.startsWith('!verexcusa') && !body.startsWith('!verexcusas')) {
                    const excTarget = await resolve(communityId, rawText, target, reply);
                    if (!excTarget) { await reply('⚠️ Menciona a alguien o escribe su personaje.'); continue; }
                    const u = await User.findOne({ groupId: communityId, userId: excTarget });
                    const label = u?.personaje ? `*${u.personaje}*` : `@${numFromJid(excTarget)}`;
                    if (!u?.excusa) { await reply(`ℹ️ ${label} sin excusa activa.`, [excTarget]); continue; }
                    await reply(`📝 *Excusa de ${label}:*\n_${u.excusa}_`, [excTarget]); continue;
                }

                // Parejas — con soporte a poliamor GRUPAL
                // !mary @user1 @user2 ... — propone a TODOS a la vez; se oficializa cuando todos acepten
                if (body.startsWith('!mary') || body.startsWith('!casar')) {
                    const uA2 = await User.findOne({ groupId: communityId, userId: authorId });
                    if (!uA2?.personaje) { await reply('❌ Necesitas tener un personaje para proponer pareja.'); continue; }
                    const allMentions = getMentions(message);
                    const { targets, ambiguous } = await resolveMultipleTargets(communityId, rawText, allMentions);
                    if (ambiguous) {
                        const lista = ambiguous.opciones.map((u,i) => `${i+1}. *${u.personaje}*`).join('\n');
                        await reply(`⚠️ Hay varios personajes llamados *${ambiguous.nombre}*:\n${lista}\n\n_Sé más específico, usa el apellido o alias._`);
                        continue;
                    }
                    if (!targets.length) { await reply('❌ Menciona a quién quieres proponer pareja.'); continue; }
                    const miNombre = primerNombre(uA2.personaje);

                    // Validar targets
                    const targetsValidos = [];
                    for (const tId of targets) {
                        if (tId === authorId) { await reply('❌ No puedes proponerte a ti mismo.'); continue; }
                        const uB2 = await User.findOne({ groupId: communityId, userId: tId });
                        if (!uB2?.personaje) { await reply(`❌ Ese usuario no tiene personaje asignado.`); continue; }
                        const yaExiste = await Pareja.findOne({ groupId: communityId, $or:[{user1:authorId,user2:tId},{user1:tId,user2:authorId}] });
                        if (yaExiste) { await reply(`💑 Ya eres pareja de *${primerNombre(uB2.personaje)}*.`); continue; }
                        targetsValidos.push(tId);
                    }
                    if (!targetsValidos.length) continue;

                    // Cancelar propuestas anteriores del mismo solicitante en este grupo
                    await SolicitudPoliamor.deleteMany({ groupId: communityId, solicitante: authorId });

                    // Crear UNA sola propuesta grupal
                    await SolicitudPoliamor.create({
                        groupId: communityId,
                        solicitante: authorId,
                        solicitados: targetsValidos,
                        aceptados: []
                    });

                    const mencionesTxt = targetsValidos.map(id => `@${numFromJid(id)}`).join(', ');
                    const esPoliamor = targetsValidos.length > 1;
                    await reply(
                        `  ┆ ⤿ 💌 ⌗ 𐔌 . ${mencionesTxt}\n\nBajo la luz de las estrellas, *${miNombre}* ha decidido que quiere compartir su vida${esPoliamor ? ' con todos ustedes' : ' contigo'}. ¿Acepta${esPoliamor ? 'n' : 's'} este lazo eterno? 💍\n\n  🌸 *!aceptar*\n  🥀 *!rechazar*\n\n  _El destino esperará solo 5 minutos..._`,
                        targetsValidos
                    );
                    continue;
                }
                // !aceptar cambio @user — aprobar solicitud de personaje
                if (body.startsWith('!aceptar cambio') && target && (isAdmin || isOW)) {
                    
                    const sol = solicitudesCambioPerso.get(target);
                    if (!sol || sol.groupId !== communityId) { await reply(`❌ No hay solicitud pendiente de @${numFromJid(target)}.`, [target]); continue; }
                    const p = sol.personaje;
                    solicitudesCambioPerso.delete(target);
                    const allU = await User.find({ groupId: communityId, personaje: { $ne: null } });
                    const normP3 = normalizarNombre(p);
                    const dup3 = allU.find(u => u.userId !== target && normalizarNombre(u.personaje || '') === normP3);
                    if (dup3) { await reply(`🚫 *"${p}"* ya fue tomado mientras esperaba.`); continue; }
                    const uT2 = await User.findOne({ groupId: communityId, userId: target });
                    const ant2 = uT2?.personaje || null;
                    if (ant2) await Disponible.create({ groupId: communityId, personaje: ant2 }).catch(()=>{});
                    await User.findOneAndUpdate({ groupId: communityId, userId: target },
                        { personaje: p, lastPersoChange: new Date(), $push: { staffLog: { accion: `Perso aprobado: ${p}`, fecha: new Date(), by: authorId } } },
                        { upsert: true, setDefaultsOnInsert: true });
                    await Disponible.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    await PersoBuscado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    await Reservado.deleteOne({ groupId: communityId, personaje: new RegExp(`^${p}$`, 'i') });
                    limpiarDupDisponibles(communityId, p).catch(() => {});
                    await react(message, '✅');
                    let rA = `  ⤷ ゛✅  ˎˊ˗\n  ♯ ·  · Cambio aprobado.\n  @${numFromJid(target)} ahora es formalmente *"${p}"*.`;
                    if (ant2) rA += `\n🕊️ *"${ant2}"* queda disponible.`;
                    await reply(rA, [target]);
                    await logAction(groupId, `Perso aprobado: ${p}`, authorId, target);
                    continue;
                }

                // !negar cambio @user — rechazar solicitud de personaje
                if (body.startsWith('!negar cambio') && target && (isAdmin || isOW)) {
                    
                    const sol2 = solicitudesCambioPerso.get(target);
                    if (!sol2 || sol2.groupId !== communityId) { await reply(`❌ No hay solicitud pendiente de @${numFromJid(target)}.`, [target]); continue; }
                    solicitudesCambioPerso.delete(target);
                    await react(message, '❌');
                    await reply(`  ⤷ ゛❌  ˎˊ˗\n  ♯ ·  · Solicitud denegada.\n  @${numFromJid(target)} se queda como estaba, ni modo.`, [target]);
                    await logAction(groupId, `Perso rechazado: ${sol2.personaje}`, authorId, target);
                    continue;
                }

                // !aceptar — aceptar solicitud de pareja / poliamor grupal
                if (body.startsWith('!aceptar') && !body.startsWith('!aceptarintercambio') && !body.startsWith('!aceptar cambio')) {
                    // Buscar propuesta grupal (SolicitudPoliamor) donde este usuario esté listado
                    const propuesta = await SolicitudPoliamor.findOne({ groupId: communityId, solicitados: authorId });
                    // Buscar también propuesta individual (SolicitudPareja) como fallback
                    const solIndividual = !propuesta
                        ? await SolicitudPareja.findOne({ groupId: communityId, solicitado: authorId })
                        : null;

                    if (!propuesta && !solIndividual) {
                        await reply('❌ No tienes solicitudes de pareja pendientes.');
                        continue;
                    }

                    if (propuesta) {
                        // ── Propuesta grupal ──
                        // Verificar que no haya aceptado ya
                        if (propuesta.aceptados.includes(authorId)) {
                            await reply('⏳ Ya aceptaste, esperando que los demás acepten también...');
                            continue;
                        }

                        // Registrar aceptación
                        propuesta.aceptados.push(authorId);
                        await propuesta.save();

                        const faltanIds = propuesta.solicitados.filter(id => !propuesta.aceptados.includes(id));

                        if (faltanIds.length > 0) {
                            // Aún faltan personas — notificar progreso
                            const uYo = await User.findOne({ groupId: communityId, userId: authorId });
                            const miNombreAcept = primerNombre(uYo?.personaje || numFromJid(authorId));
                            const faltan = await Promise.all(faltanIds.map(async id => {
                                const u = await User.findOne({ groupId: communityId, userId: id });
                                return `*${primerNombre(u?.personaje || numFromJid(id))}*`;
                            }));
                            await react(message, '💫');
                            await reply(
                                `  ┆ ⤿ 💌 . *${miNombreAcept}* aceptó 💍\n  _Esperando a: ${faltan.join(', ')}..._`,
                                faltanIds
                            );
                            continue;
                        }

                        // ── TODOS aceptaron — oficializar ──
                        const uSol = await User.findOne({ groupId: communityId, userId: propuesta.solicitante });
                        const nombreSol = primerNombre(uSol?.personaje || numFromJid(propuesta.solicitante));

                        const todosIds = [propuesta.solicitante, ...propuesta.solicitados];

                        // Crear los lazos de pareja entre solicitante y cada aceptado
                        for (const aceptadoId of propuesta.solicitados) {
                            const yaExisteP = await Pareja.findOne({ groupId: communityId, $or:[{user1:propuesta.solicitante,user2:aceptadoId},{user1:aceptadoId,user2:propuesta.solicitante}] });
                            if (!yaExisteP) await Pareja.create({ groupId: communityId, user1: propuesta.solicitante, user2: aceptadoId });
                        }
                        // Si es poliamor (3+), crear lazos entre todos los aceptados también
                        if (propuesta.solicitados.length > 1) {
                            for (let i = 0; i < propuesta.solicitados.length; i++) {
                                for (let j = i + 1; j < propuesta.solicitados.length; j++) {
                                    const a = propuesta.solicitados[i], b = propuesta.solicitados[j];
                                    const yaExisteAB = await Pareja.findOne({ groupId: communityId, $or:[{user1:a,user2:b},{user1:b,user2:a}] });
                                    if (!yaExisteAB) await Pareja.create({ groupId: communityId, user1: a, user2: b });
                                }
                            }
                        }
                        await SolicitudPoliamor.deleteOne({ _id: propuesta._id });

                        const nombresAcept = await Promise.all(propuesta.solicitados.map(async id => {
                            const u = await User.findOne({ groupId: communityId, userId: id });
                            return `*${primerNombre(u?.personaje || numFromJid(id))}*`;
                        }));

                        const esPoliamor = propuesta.solicitados.length > 1;
                        await react(message, '💑');
                        await reply(
                            `  ⤷ ゛💍  ˎˊ˗\n  ♯ ·  · Un hilo rojo acaba de unirse.\n  *${nombreSol}* ${esPoliamor ? 'y' : 'y'} ${nombresAcept.join(' y ')} ahora caminan juntos. ¡Que viva el amor! ✨`,
                            todosIds
                        );
                        continue;
                    }

                    // ── Fallback: propuesta individual (SolicitudPareja) ──
                    const uMe2 = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNombreA = primerNombre(uMe2?.personaje || numFromJid(authorId));
                    const yaExisteP2 = await Pareja.findOne({ groupId: communityId, $or:[{user1:solIndividual.solicitante,user2:authorId},{user1:authorId,user2:solIndividual.solicitante}] });
                    if (!yaExisteP2) await Pareja.create({ groupId: communityId, user1: solIndividual.solicitante, user2: authorId });
                    await SolicitudPareja.deleteOne({ _id: solIndividual._id });
                    const uSolInd = await User.findOne({ groupId: communityId, userId: solIndividual.solicitante });
                    const nombreSolInd = primerNombre(uSolInd?.personaje || numFromJid(solIndividual.solicitante));
                    await react(message, '💑');
                    await reply(`  ⤷ ゛💍  ˎˊ˗\n  ♯ ·  · Un hilo rojo acaba de unirse.\n  *${nombreSolInd}* y *${miNombreA}* ahora caminan juntos. ¡Que viva el amor! ✨`, [solIndividual.solicitante]);
                    continue;
                }

                // !rechazar — rechazar solicitud(es) de pareja
                if (body.startsWith('!rechazar')) {
                    const allMentionsRej = getMentions(message);
                    const { targets: targetsRej, ambiguous: ambigRej } = await resolveMultipleTargets(communityId, rawText, allMentionsRej);
                    if (ambigRej) {
                        const lista = ambigRej.opciones.map((u,i) => `${i+1}. *${u.personaje}*`).join('\n');
                        await reply(`⚠️ Hay varios personajes llamados *${ambigRej.nombre}*:\n${lista}\n\n_Sé más específico._`);
                        continue;
                    }
                    const todasSolRej = await SolicitudPareja.find({ groupId: communityId, solicitado: authorId });
                    if (!todasSolRej.length) { await reply('❌ No tienes solicitudes pendientes.'); continue; }

                    let aRechazar = targetsRej.length
                        ? todasSolRej.filter(s => targetsRej.includes(s.solicitante))
                        : todasSolRej; // sin target → rechaza todas

                    if (!aRechazar.length) { await reply('❌ No hay solicitud de esa persona.'); continue; }

                    const uMeRej = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNombreR = primerNombre(uMeRej?.personaje || numFromJid(authorId));
                    const nombresRej = await Promise.all(aRechazar.map(async s => {
                        const u = await User.findOne({ groupId: communityId, userId: s.solicitante });
                        return `*${primerNombre(u?.personaje || numFromJid(s.solicitante))}*`;
                    }));
                    for (const sol of aRechazar) await SolicitudPareja.deleteOne({ _id: sol._id });
                    await react(message,'✅');
                    await reply(`  ⤷ ゛🥀  ˎˊ˗\n  ♯ ·  · El corazón de *${miNombreR}* se rompió.\n  Ha rechazado a ${nombresRej.join(' y ')}...\n  A veces, el "siempre" no es para todos.`);
                    continue;
                }

                // !divorcio — terminar pareja(s), con soporte a poliamor
                if (body.startsWith('!divorcio')) {
                    const allMentionsDiv = getMentions(message);
                    const { targets: targetsDiv, ambiguous: ambigDiv } = await resolveMultipleTargets(communityId, rawText, allMentionsDiv);
                    if (ambigDiv) {
                        const lista = ambigDiv.opciones.map((u,i) => `${i+1}. *${u.personaje}*`).join('\n');
                        await reply(`⚠️ Hay varios personajes llamados *${ambigDiv.nombre}*:\n${lista}\n\n_Sé más específico._`);
                        continue;
                    }
                    const misParej = await Pareja.find({ groupId: communityId, $or:[{user1:authorId},{user2:authorId}] });
                    if (!misParej.length) { await reply('ℹ️ No tienes pareja.'); continue; }

                    let aDivorciarse = targetsDiv.length
                        ? misParej.filter(p => targetsDiv.includes(p.user1 === authorId ? p.user2 : p.user1))
                        : misParej; // sin target → divorcio de todos

                    if (!aDivorciarse.length) { await reply('❌ No tienes pareja con esa persona.'); continue; }

                    const uMyD = await User.findOne({ groupId: communityId, userId: authorId });
                    const miNombreD = primerNombre(uMyD?.personaje || numFromJid(authorId));
                    const exNombres = await Promise.all(aDivorciarse.map(async p => {
                        const exId = p.user1 === authorId ? p.user2 : p.user1;
                        const uEx = await User.findOne({ groupId: communityId, userId: exId });
                        return `*${primerNombre(uEx?.personaje || numFromJid(exId))}*`;
                    }));
                    for (const p of aDivorciarse) await Pareja.deleteOne({ _id: p._id });
                    await react(message,'✅');
                    await reply(`  ⤷ ゛🍂  ˎˊ˗\n  ♯ ·  · En Saturno, viven los hijos que nunca tuvimos...\n\n  *${miNombreD}* y ${exNombres.join(' y ')} se han dicho adiós.\n  Y donde quedó, ese botón? Que lleva a la felicidad... Luna de miel, rosa pastel.`);
                    continue;
                }
            } catch (err) {
                console.error('· msg error:', err.message, '| cmd:', (typeof body !== 'undefined' ? body?.slice(0,30) : 'n/a'), '| user:', numFromJid((typeof authorId !== 'undefined' ? authorId : '') || ''));
            }
        }
    });

    // ── ViewOnce — interceptar solo mensajes con media viewOnce real ──
    sock.ev.on('messages.update', async (updates) => {
        for (const update of updates) {
            try {
                if (!update.key?.remoteJid?.endsWith('@g.us')) continue;
                const groupId = update.key.remoteJid;
                const meta = await getMeta(groupId);
                if (!meta) continue;
                const communityId = meta?.linkedParent || groupId;
                let cfg = await Config.findOne({ groupId });
                if (!cfg?.antiporno) continue;

                // Solo procesar si el update contiene viewOnce con media real
                const msg = update.update?.message;
                if (!msg) continue; // ignorar updates sin mensaje (leídos, reacciones, etc.)
                const viewOnceMsg = msg?.viewOnceMessage?.message
                    || msg?.viewOnceMessageV2?.message
                    || msg?.viewOnceMessageV2Extension?.message;
                // Validación estricta: debe ser viewOnce con imagen o video real
                if (!viewOnceMsg?.imageMessage && !viewOnceMsg?.videoMessage) continue;
                // Ignorar si no tiene url de descarga (media vacío)
                const mediaCheck = viewOnceMsg?.imageMessage || viewOnceMsg?.videoMessage;
                if (!mediaCheck?.url && !mediaCheck?.directPath) continue;

                const authorId = update.key.participant;
                if (!authorId) continue;
                if (BOT_NUM && numFromJid(authorId) === BOT_NUM) continue;

                console.log('· viewOnce detectado de', numFromJid(authorId));

                // Calcular tier del remitente para ajustar sensibilidad
                const voUserData = await User.findOne({ groupId: communityId, userId: authorId });
                const voIsOwner  = await isOwner(authorId);
                const voTier = (() => {
                    if (voIsOwner) return 1.15;
                    if (!voUserData) return 0.90;
                    if (voUserData.confi) return 1.08;
                    if (voUserData.desconfi) return 0.90;
                    if (voUserData.desconfiSince) {
                        const dias = (Date.now() - new Date(voUserData.desconfiSince)) / 86400000;
                        if (dias < 7) return 0.90;
                    }
                    return 1.0;
                })();

                // Parchear viewOnce flag para maximizar tasa de descarga
                const fakeUpdateMsg = { key: update.key, message: msg };
                const voRes = await downloadViewOnce(fakeUpdateMsg);
                let buffer = voRes.buffer, frames = voRes.frames;

                if (!buffer && !frames.length) { console.log('· viewOnce: no se pudo descargar — no se sanciona'); continue; }

                let bad = false, tipoContenido = '', tipoDetalle = '';
                const checkVO = async (buf) => {
                    if (bad) return;
                    const r = await queryNudeNet(buf);
                    if (!r) return;
                    const { tipo } = analizarResultadoNudeNet(r, false, voTier);
                    if (tipo) {
                        bad = true;
                        tipoContenido = tipo === 'gore' ? 'gore' : 'contenido explícito (ver una vez)';
                        tipoDetalle = '';
                    }
                };
                if (buffer) await checkVO(buffer);
                else await Promise.all(frames.map(f => checkVO(f)));

                if (!bad) continue;

                // Borrar viewOnce — necesita key completa con fromMe:false
                try {
                    await sock.sendMessage(groupId, {
                        delete: {
                            remoteJid: groupId,
                            fromMe: false,
                            id: update.key.id,
                            participant: authorId
                        }
                    });
                } catch(e) {
                    console.error('· viewOnce delete error:', e.message);
                    // Fallback: intentar con la key original
                    try { await sock.sendMessage(groupId, { delete: { ...update.key, fromMe: false } }); } catch(_) {}
                }

                const existing = await User.findOne({ groupId: communityId, userId: authorId });
                const adminsAlert = (meta?.participants||[]).filter(p => (p.admin==='admin'||p.admin==='superadmin') && p.id !== authorId).map(p=>p.id);
                const tLabel = existing?.personaje ? `*${existing.personaje}*` : `@${numFromJid(authorId)}`;

                if ((existing?.advs || 0) < MAX_ADV) {
                    const u = await User.findOneAndUpdate(
                        { groupId: communityId, userId: authorId },
                        { $inc: { advs: 1 }, $push: {
                            warnLog:  { motivo: tipoContenido, fecha: new Date(), by: 'bot' },
                            staffLog: { accion: `Auto: ${tipoContenido}`, fecha: new Date(), by: 'bot' }
                        }},
                        { returnDocument: 'after', upsert: true, setDefaultsOnInsert: true }
                    );
                    await logAction(groupId, `Auto: ${tipoContenido} — ${tipoDetalle}`, 'bot', authorId);
                    await notificarAdv(groupId, u, `${tipoContenido}${tipoDetalle ? ` (${tipoDetalle})` : ''}`, 'bot', meta, authorId);
                } else {
                    await sock.sendMessage(groupId,
                        { text: `╭━━━━━━━━━━━━━━━━━━━━━━━\n🚨 ALERTA STAFF · ${tLabel} — @${numFromJid(authorId)}\n\n 📄: _${tipoContenido}_\n\n ${adminsAlert.slice(0,3).map(a=>`@${numFromJid(a)}`).join(' ')}\n╰━━━━━━━━━━━━━━━━━━━━━━━`,
                        mentions: [authorId, ...adminsAlert.slice(0,3)] });
                }
            } catch(e) { console.error('· viewOnce update error:', e.message); }
        }
    });
}

http.createServer((req, res) => res.end('beyonder ok')).listen(process.env.PORT || 3000, () => {
    console.log(`· http :{${process.env.PORT || 3000}}`);
});

startBot();
